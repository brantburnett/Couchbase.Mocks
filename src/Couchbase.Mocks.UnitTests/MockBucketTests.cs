using System;
using System.Collections.Generic;
using Couchbase.IO;
using Xunit;

namespace Couchbase.Mocks.UnitTests
{
    public class MockBucketTests
    {
        #region Get

        public static IEnumerable<object[]> MissingDocs()
        {
            yield return new object[]
            {
                Array.Empty<KeyValuePair<string, MockDocument>>()
            };

            yield return new object[]
            {
                new[]
                {
                    new KeyValuePair<string, MockDocument>("key", null)
                }
            };

            yield return new object[]
            {
                new[]
                {
                    new KeyValuePair<string, MockDocument>("key", new MockDocument
                    {
                        Content = new object(),
                        Cas = 100,
                        Expiration = new DateTime(2000, 1, 1)
                    })
                }
            };
        }

        [Theory]
        [MemberData(nameof(MissingDocs))]
        public void Get_MissingDoc_KeyNotFound(IEnumerable<KeyValuePair<string, MockDocument>> documents)
        {
            // Arrange

            // Will ensure sample doc above on 1/1/2000 is expired
            var clockProvider = new LambdaClockProvider(() => new DateTime(2000, 1, 2));

            var bucket = new MockBucket(clockProvider);
            bucket.AddRange(documents);

            // Act

            var result = bucket.Get<object>("key");

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.KeyNotFound, result.Status);
        }

        [Fact]
        public void Get_ExistingDoc_ReturnsDoc()
        {
            // Arrange

            var existingObject = new object();
            var bucket = new MockBucket
            {
                {"key", existingObject}
            };

            // Act

            var result = bucket.Get<object>("key");

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.Equal(existingObject, mockDocument?.Content);
        }

        #endregion

        #region Insert

        [Fact]
        public void Insert_NewDoc_Success()
        {
            // Arrange

            var bucket = new MockBucket();

            // Act

            var result = bucket.Insert("key", new object());

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.ContainsMock("key"));
        }

        [Fact]
        public void Insert_ExistingDoc_KeyExists()
        {
            // Arrange

            var existingObject = new object();
            var bucket = new MockBucket
            {
                {"key", existingObject}
            };

            // Act

            var result = bucket.Insert("key", new object());

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.KeyExists, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.Equal(existingObject, mockDocument?.Content);
        }

        #endregion

        #region Remove

        [Fact]
        public void Remove_NewDoc_KeyNotFound()
        {
            // Arrange

            var bucket = new MockBucket();

            // Act

            var result = bucket.Remove("key");

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.KeyNotFound, result.Status);
        }

        [Fact]
        public void Remove_ExistingDoc_Removes()
        {
            // Arrange

            var bucket = new MockBucket
            {
                {"key", new object()}
            };

            // Act

            var result = bucket.Remove("key");

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.False(bucket.TryGetMock("key", out _));
        }

        [Fact]
        public void Remove_DifferentCas_CasMismatch()
        {
            // Arrange

            var existingObject = new object();
            var bucket = new MockBucket
            {
                {"key", existingObject}
            };

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            var originalCas = mockDocument.Cas;

            // Act

            var result = bucket.Remove("key", originalCas - 1);

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.DocumentMutationDetected, result.Status);

            Assert.True(bucket.TryGetMock("key", out mockDocument));
            Assert.Equal(existingObject, mockDocument?.Content);
        }

        #endregion

        #region Replace

        [Fact]
        public void Replace_NewDoc_KeyNotFound()
        {
            // Arrange

            var bucket = new MockBucket();

            // Act

            var result = bucket.Replace("key", new object());

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.KeyNotFound, result.Status);
        }

        [Fact]
        public void Replace_ExistingDoc_Overwrites()
        {
            // Arrange

            var bucket = new MockBucket
            {
                {"key", new object()}
            };

            var newObject = new object();

            // Act

            var result = bucket.Replace("key", newObject);

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.Equal(newObject, mockDocument?.Content);
        }

        [Fact]
        public void Replace_ExistingDoc_IncrementsCas()
        {
            // Arrange

            var bucket = new MockBucket
            {
                {"key", new object()}
            };

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            var originalCas = mockDocument.Cas;

            // Act

            bucket.Replace("key", new object());

            // Assert

            Assert.True(bucket.TryGetMock("key", out mockDocument));
            Assert.True(mockDocument.Cas > originalCas);
        }

        [Fact]
        public void Replace_DifferentCas_CasMismatch()
        {
            // Arrange

            var existingObject = new object();
            var bucket = new MockBucket
            {
                {"key", existingObject}
            };

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            var originalCas = mockDocument.Cas;

            // Act

            var result = bucket.Replace("key", new object(), originalCas - 1);

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.DocumentMutationDetected, result.Status);

            Assert.True(bucket.TryGetMock("key", out mockDocument));
            Assert.Equal(existingObject, mockDocument?.Content);
        }

        #endregion

        #region Touch

        [Theory]
        [MemberData(nameof(MissingDocs))]
        public void Touch_MissingDoc_KeyNotFound(IEnumerable<KeyValuePair<string, MockDocument>> documents)
        {
            // Arrange

            // Will ensure sample doc above on 1/1/2000 is expired
            var clockProvider = new LambdaClockProvider(() => new DateTime(2000, 1, 2));

            var bucket = new MockBucket(clockProvider);
            bucket.AddRange(documents);

            // Act

            var result = bucket.Touch("key", TimeSpan.FromMinutes(1));

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.KeyNotFound, result.Status);
        }

        [Fact]
        public void Touch_ExistingDocNoExpiration_RemovesExpiration()
        {
            // Arrange

            var clockProvider = new LambdaClockProvider(() => new DateTime(2000, 1, 1));

            var bucket = new MockBucket(clockProvider)
            {
                {"key", new MockDocument
                {
                    Cas = 100,
                    Content = new object(),
                    Expiration = new DateTime(2000, 1, 2)
                }}
            };

            // Act

            var result = bucket.Touch("key", TimeSpan.Zero);

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.Null(mockDocument.Expiration);
        }

        [Fact]
        public void Touch_ExistingDocWithExpiration_SetsExpiration()
        {
            // Arrange

            var clockProvider = new LambdaClockProvider(() => new DateTime(2000, 1, 1));

            var bucket = new MockBucket(clockProvider)
            {
                {"key", new MockDocument
                {
                    Cas = 100,
                    Content = new object(),
                    Expiration = new DateTime(2000, 1, 2)
                }}
            };

            // Act

            var result = bucket.Touch("key", TimeSpan.FromMinutes(1));

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.NotNull(mockDocument.Expiration);
            Assert.Equal(clockProvider.GetCurrentDateTime().AddMinutes(1), mockDocument.Expiration.Value);
        }

        #endregion

        #region Upsert

        [Fact]
        public void Upsert_NewDoc_KeyNotFound()
        {
            // Arrange

            var bucket = new MockBucket();

            // Act

            var result = bucket.Upsert("key", new object());

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.ContainsMock("key"));
        }

        [Fact]
        public void Upsert_ExistingDoc_Overwrites()
        {
            // Arrange

            var bucket = new MockBucket
            {
                {"key", new object()}
            };

            var newObject = new object();

            // Act

            var result = bucket.Upsert("key", newObject);

            // Assert

            Assert.True(result.Success);
            Assert.Equal(ResponseStatus.Success, result.Status);

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            Assert.Equal(newObject, mockDocument?.Content);
        }

        [Fact]
        public void Upsert_ExistingDoc_IncrementsCas()
        {
            // Arrange

            var bucket = new MockBucket
            {
                {"key", new object()}
            };

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            var originalCas = mockDocument.Cas;

            // Act

            bucket.Upsert("key", new object());

            // Assert

            Assert.True(bucket.TryGetMock("key", out mockDocument));
            Assert.True(mockDocument.Cas > originalCas);
        }

        [Fact]
        public void Upsert_DifferentCas_CasMismatch()
        {
            // Arrange

            var existingObject = new object();
            var bucket = new MockBucket
            {
                {"key", existingObject}
            };

            Assert.True(bucket.TryGetMock("key", out var mockDocument));
            var originalCas = mockDocument.Cas;

            // Act

            var result = bucket.Upsert("key", new object(), originalCas - 1);

            // Assert

            Assert.False(result.Success);
            Assert.Equal(ResponseStatus.DocumentMutationDetected, result.Status);

            Assert.True(bucket.TryGetMock("key", out mockDocument));
            Assert.Equal(existingObject, mockDocument?.Content);
        }

        #endregion
    }
}

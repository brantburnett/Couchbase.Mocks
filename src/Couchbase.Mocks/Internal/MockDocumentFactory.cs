using System;
using System.Collections.Generic;
using System.Text;

namespace Couchbase.Mocks.Internal
{
    internal class MockDocumentFactory
    {
        private readonly IClockProvider _clockProvider;

        public MockDocumentFactory(IClockProvider clockProvider)
        {
            _clockProvider = clockProvider ?? throw new ArgumentNullException(nameof(clockProvider));
        }

        public MockDocument Create(object content, TimeSpan expiration)
        {
            return new MockDocument
            {
                Content = content,
                Cas = 100,
                Expiration = expiration > TimeSpan.Zero
                    ? _clockProvider.GetCurrentDateTime().Add(expiration)
                    : (DateTime?) null
            };
        }
    }
}

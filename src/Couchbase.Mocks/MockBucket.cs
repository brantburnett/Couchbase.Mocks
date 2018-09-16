using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Couchbase.Analytics;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.Core.Buckets;
using Couchbase.Core.Monitoring;
using Couchbase.Core.Version;
using Couchbase.IO;
using Couchbase.IO.Operations;
using Couchbase.Management;
using Couchbase.Mocks.Internal;
using Couchbase.N1QL;
using Couchbase.Search;
using Couchbase.Views;

namespace Couchbase.Mocks
{
    public class MockBucket :
        IBucket,
        IEnumerable<KeyValuePair<string, MockDocument>>
    {
        private static readonly TimeSpan DefaultOperationTimeout = TimeSpan.MaxValue;

        private readonly ConcurrentDictionary<string, MockDocument> _bucket =
            new ConcurrentDictionary<string, MockDocument>();

        private readonly IClockProvider _clockProvider;
        private readonly MockDocumentFactory _mockDocumentFactory;

        private bool _disposed;

        public string Name { get; }
        public BucketTypeEnum BucketType { get; set; } = BucketTypeEnum.Couchbase;
        public ICluster Cluster { get; set; }
        public bool IsSecure { get; set; }
        public bool SupportsEnhancedDurability { get; set; } = true;
        public bool SupportsKvErrorMap { get; set; } = true;
        public BucketConfiguration Configuration { get; set; }

        public ClusterVersion ClusterVersion { get; set; } = new ClusterVersion(new Version(5, 5, 0));

        #region Constructors

        public MockBucket() :
            this("default")
        {
        }

        public MockBucket(string name) :
            this(name, new LambdaClockProvider(() => DateTime.Now))
        {
        }

        public MockBucket(IClockProvider clockProvider) :
            this("default", clockProvider)
        {
        }

        public MockBucket(string name, IClockProvider clockProvider)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            _clockProvider = clockProvider ?? throw new ArgumentNullException(nameof(clockProvider));

            _mockDocumentFactory = new MockDocumentFactory(_clockProvider);
        }

        #endregion

        #region Core Implementations

        public bool Exists(string key, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return _bucket.ContainsKey(key);
        }

        public IOperationResult<T> Get<T>(string key, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return InternalGet(key,
                currentValue => new MockOperationResult<T>
                {
                    Cas = currentValue.Cas,
                    Id = key,
                    OpCode = OperationCode.Get,
                    Status = ResponseStatus.Success,
                    Success = true,
                    Value = (T) currentValue.Content
                },
                () => new MockOperationResult<T>
                {
                    Id = key,
                    OpCode = OperationCode.Touch,
                    Status = ResponseStatus.KeyNotFound,
                    Success = false
                });
        }

        public IOperationResult<T> GetAndTouch<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return InternalGet(key,
                currentValue =>
                {
                    currentValue.Expiration = GetNewExpiration(expiration);

                    return new MockOperationResult<T>
                    {
                        Cas = currentValue.Cas,
                        Id = key,
                        OpCode = OperationCode.GAT,
                        Status = ResponseStatus.Success,
                        Success = true,
                        Value = (T) currentValue.Content
                    };
                },
                () => new MockOperationResult<T>
                {
                    Id = key,
                    OpCode = OperationCode.GAT,
                    Status = ResponseStatus.KeyNotFound,
                    Success = false
                });
        }

        public IOperationResult<T> GetFromReplica<T>(string key, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return Get<T>(key, timeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            EnsureNotDisposed();

            var doc = _mockDocumentFactory.Create(value, expiration);

            return AddOrUpdate(key, doc,
                () => new MockOperationResult<T>
                {
                    Cas = doc.Cas,
                    Durability = GetSuccessfulDurability(replicateTo, persistTo),
                    Id = key,
                    OpCode = OperationCode.Add,
                    Status = ResponseStatus.Success,
                    Success = true,
                    Value = value
                },
                _ => new MockOperationResult<T>
                {
                    Durability = Durability.Unspecified,
                    Id = key,
                    OpCode = OperationCode.Add,
                    Status = ResponseStatus.KeyExists,
                    Success = false
                });
        }

        public IOperationResult Remove(string key, ulong cas, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return AddOrUpdate(key, null,
                () => new MockOperationResult
                {
                    Durability = Durability.Unspecified,
                    Id = key,
                    OpCode = OperationCode.Delete,
                    Status = ResponseStatus.KeyNotFound,
                    Success = false
                },
                currentValue =>
                {
                    if (cas != 0 && currentValue.Cas != cas)
                    {
                        return new MockOperationResult
                        {
                            Durability = Durability.Unspecified,
                            Id = key,
                            OpCode = OperationCode.Delete,
                            Status = ResponseStatus.DocumentMutationDetected,
                            Success = false
                        };
                    }
                    else
                    {
                        return new MockOperationResult
                        {
                            Durability = GetSuccessfulDurability(replicateTo, persistTo),
                            Id = key,
                            OpCode = OperationCode.Delete,
                            Status = ResponseStatus.Success,
                            Success = true
                        };
                    }
                });
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            EnsureNotDisposed();

            var doc = _mockDocumentFactory.Create(value, expiration);

            return AddOrUpdate(key, doc,
                () => new MockOperationResult<T>
                {
                    Durability = Durability.Unspecified,
                    Id = key,
                    OpCode = OperationCode.Replace,
                    Status = ResponseStatus.KeyNotFound,
                    Success = false
                },
                currentValue =>
                {
                    if (cas != 0 && currentValue.Cas != cas)
                    {
                        return new MockOperationResult<T>
                        {
                            Durability = Durability.Unspecified,
                            Id = key,
                            OpCode = OperationCode.Replace,
                            Status = ResponseStatus.DocumentMutationDetected,
                            Success = false
                        };
                    }
                    else
                    {
                        doc.Cas = currentValue.Cas + 1;

                        return new MockOperationResult<T>
                        {
                            Cas = doc.Cas,
                            Durability = GetSuccessfulDurability(replicateTo, persistTo),
                            Id = key,
                            OpCode = OperationCode.Replace,
                            Status = ResponseStatus.Success,
                            Success = true,
                            Value = value
                        };
                    }
                });
        }

        public IOperationResult Touch(string key, TimeSpan expiration, TimeSpan timeout)
        {
            EnsureNotDisposed();

            return InternalGet(key,
                currentValue =>
                {
                    currentValue.Expiration = GetNewExpiration(expiration);

                    return new MockOperationResult
                    {
                        Cas = currentValue.Cas,
                        Id = key,
                        OpCode = OperationCode.Touch,
                        Status = ResponseStatus.Success,
                        Success = true
                    };
                },
                () => new MockOperationResult
                {
                    Id = key,
                    OpCode = OperationCode.Touch,
                    Status = ResponseStatus.KeyNotFound,
                    Success = false
                });
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            EnsureNotDisposed();

            var doc = _mockDocumentFactory.Create(value, expiration);

            return AddOrUpdate(key, doc,
                () => new MockOperationResult<T>
                {
                    Cas = doc.Cas,
                    Durability = GetSuccessfulDurability(replicateTo, persistTo),
                    Id = key,
                    OpCode = OperationCode.Replace,
                    Status = ResponseStatus.Success,
                    Success = true,
                    Value = value
                },
                currentValue =>
                {
                    if (cas != 0 && currentValue.Cas != cas)
                    {
                        return new MockOperationResult<T>
                        {
                            Durability = Durability.Unspecified,
                            Id = key,
                            OpCode = OperationCode.Replace,
                            Status = ResponseStatus.DocumentMutationDetected,
                            Success = false
                        };
                    }
                    else
                    {
                        doc.Cas = currentValue.Cas + 1;

                        return new MockOperationResult<T>
                        {
                            Cas = doc.Cas,
                            Durability = GetSuccessfulDurability(replicateTo, persistTo),
                            Id = key,
                            OpCode = OperationCode.Replace,
                            Status = ResponseStatus.Success,
                            Success = true,
                            Value = value
                        };
                    }
                });
        }

        #endregion

        #region Core IDocument Implementations

        public IDocumentResult<T> GetAndTouchDocument<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            var result = GetAndTouch<T>(key, timeout);

            return new DocumentResult<T>(result);
        }

        public IDocumentResult<T> GetDocument<T>(string id, TimeSpan timeout)
        {
            var result = Get<T>(id, timeout);

            return new DocumentResult<T>(result);
        }

        public IDocumentResult<T> GetDocumentFromReplica<T>(string id, TimeSpan timeout)
        {
            var result = GetFromReplica<T>(id, timeout);

            return new DocumentResult<T>(result);
        }

        public IDocumentResult<T> Insert<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            var result = Insert(document.Id, document.Content, TimeSpan.FromMilliseconds(document.Expiry),
                replicateTo, persistTo, timeout);

            return new DocumentResult<T>(result);
        }

        public IOperationResult Remove<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Remove(document.Id, document.Cas, replicateTo, persistTo, timeout);
        }

        public IDocumentResult<T> Replace<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            var result = Replace(document.Id, document.Content, document.Cas,
                TimeSpan.FromMilliseconds(document.Expiry), replicateTo, persistTo, timeout);

            return new DocumentResult<T>(result);
        }

        public IDocumentResult<T> Upsert<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            var result = Upsert(document.Id, document.Content, document.Cas,
                TimeSpan.FromMilliseconds(document.Expiry), replicateTo, persistTo, timeout);

            return new DocumentResult<T>(result);
        }

        #endregion

        #region Multi-Op Core Implementations

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys, ParallelOptions options,
            int rangeSize, TimeSpan timeout)
        {
            var result = new ConcurrentDictionary<string, IOperationResult<T>>();

            if (keys != null && keys.Count > 0)
            {
                Parallel.ForEach(keys, options, key => { result.TryAdd(key, Get<T>(key, timeout)); });
            }

            return result;
        }

        public Task<IDocumentResult<T>[]> GetDocumentsAsync<T>(IEnumerable<string> ids, TimeSpan timeout)
        {
            return Task.FromResult(
                ids.Select(id => GetDocument<T>(id, timeout)).ToArray());
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                documents.Select(p => Insert(p, replicateTo, persistTo, timeout)).ToArray());
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys, ParallelOptions options, int rangeSize,
            TimeSpan timeout)
        {
            var result = new ConcurrentDictionary<string, IOperationResult>();

            if (keys != null && keys.Count > 0)
            {
                Parallel.ForEach(keys, options, key => { result.TryAdd(key, Remove(key, timeout)); });
            }

            return result;
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                documents.Select(p => Remove(p, replicateTo, persistTo, timeout)).ToArray());
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                documents.Select(p => Replace(p, replicateTo, persistTo, timeout)).ToArray());
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items, ParallelOptions options,
            int rangeSize, TimeSpan timeout)
        {
            var result = new ConcurrentDictionary<string, IOperationResult<T>>();

            if (items != null && items.Count > 0)
            {
                Parallel.ForEach(items, options, pair => { result.TryAdd(pair.Key, Upsert(pair.Key, pair.Value)); });
            }

            return result;
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                documents.Select(p => Upsert(p, replicateTo, persistTo, timeout)).ToArray());
        }

        #endregion

        #region IDictionary Like Members

        private IEnumerable<KeyValuePair<string, MockDocument>> ActiveDocuments {
            get
            {
                EnsureNotDisposed();

                var now = _clockProvider.GetCurrentDateTime();

                return _bucket
                    .Where(p => p.Value != null && !p.Value.IsExpired(now));
            }
        }

        public int Count => ActiveDocuments.Count();
        public ICollection<string> Keys => ActiveDocuments.Select(p => p.Key).ToList();
        public ICollection<MockDocument> Documents => ActiveDocuments.Select(p => p.Value).ToList();

        public void Add(string key, object content)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (content == null)
            {
                throw new ArgumentNullException(nameof(content));
            }

            Add(key, _mockDocumentFactory.Create(content, TimeSpan.Zero));
        }

        public void Add(string key, MockDocument document)
        {
            _bucket.TryAdd(key, document);
        }

        public void AddRange(IEnumerable<KeyValuePair<string, object>> documents)
        {
            if (documents == null)
            {
                throw new ArgumentNullException(nameof(documents));
            }

            foreach (var document in documents)
            {
                Add(document.Key, document.Value);
            }
        }

        public void AddRange(IEnumerable<KeyValuePair<string, MockDocument>> documents)
        {
            if (documents == null)
            {
                throw new ArgumentNullException(nameof(documents));
            }

            foreach (var document in documents)
            {
                Add(document.Key, document.Value);
            }
        }

        public bool ContainsMock(string key)
        {
            if (!_bucket.TryGetValue(key, out var document))
            {
                return false;
            }

            return document != null;
        }

        public bool TryGetMock(string key, out MockDocument document)
        {
            return _bucket.TryGetValue(key, out document) && document != null;
        }

        public bool RemoveMock(string key)
        {
            return _bucket.TryRemove(key, out _);
        }

        public void Flush()
        {
            _bucket.Clear();
        }

        #endregion

        #region IEnumerable

        public IEnumerator<KeyValuePair<string, MockDocument>> GetEnumerator()
        {
            EnsureNotDisposed();

            return ActiveDocuments.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Dispose

        public void Dispose()
        {
            // We don't need to dispose anything, but we want to replicate ObjectDisposedException
            _disposed = true;
        }

        private void EnsureNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MockBucket));
            }
        }

        #endregion

        #region Exists

        public bool Exists(string key)
        {
            return Exists(key, DefaultOperationTimeout);
        }

        public Task<bool> ExistsAsync(string key)
        {
            return Task.FromResult(Exists(key));
        }

        public Task<bool> ExistsAsync(string key, TimeSpan timeout)
        {
            return Task.FromResult(Exists(key));
        }

        #endregion

        #region Observe

        public Task<ObserveResponse> ObserveAsync(string key, ulong cas, bool deletion, ReplicateTo replicateTo, PersistTo persistTo)
        {
            throw new NotImplementedException();
        }

        public Task<ObserveResponse> ObserveAsync(string key, ulong cas, bool deletion, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public ObserveResponse Observe(string key, ulong cas, bool deletion, ReplicateTo replicateTo, PersistTo persistTo)
        {
            throw new NotImplementedException();
        }

        public ObserveResponse Observe(string key, ulong cas, bool deletion, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Upsert

        public IDocumentResult<T> Upsert<T>(IDocument<T> document)
        {
            return Upsert(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Upsert<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Upsert(document, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document)
        {
            return Task.FromResult(Upsert(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Task.FromResult(Upsert(document, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Upsert<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Upsert(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Upsert<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Upsert(document, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Task.FromResult(Upsert(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(Upsert(document, replicateTo, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Upsert<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Upsert(document, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(Upsert(document, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> UpsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(Upsert(document, replicateTo, persistTo, timeout));
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents)
        {
            return UpsertAsync(documents, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents, TimeSpan timeout)
        {
            return UpsertAsync(documents, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo)
        {
            return UpsertAsync(documents, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return UpsertAsync(documents, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> UpsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return UpsertAsync(documents, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value)
        {
            return Upsert(key, value, 0, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.Zero,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, uint expiration)
        {
            return Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, uint expiration)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, TimeSpan expiration)
        {
            return Upsert(key, value, 0, expiration,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Upsert(key, value, 0, expiration,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, TimeSpan expiration)
        {
            return Task.FromResult(
                Upsert(key, value, 0, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas)
        {
            return Upsert(key, value, cas, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas)
        {
            return Task.FromResult(
                Upsert(key, value, cas, TimeSpan.Zero,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, uint expiration)
        {
            return Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, uint expiration, TimeSpan timeout)
        {
            return Upsert(key, value, cas, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, uint expiration)
        {
            return Task.FromResult(
                Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, uint expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, TimeSpan expiration)
        {
            return Upsert(key, value, cas, expiration,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, TimeSpan expiration, TimeSpan timeout)
        {
            return Upsert(key, value, cas, expiration,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, TimeSpan expiration)
        {
            return Task.FromResult(
                Upsert(key, value, cas, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, cas, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Upsert(key, value, 0, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Upsert(key, value, 0, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Upsert(key, value, 0, TimeSpan.Zero,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Upsert(key, value, 0, TimeSpan.Zero,
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.Zero,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.Zero,
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo,
            PersistTo persistTo)
        {
            return Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Upsert(key, value, 0, expiration,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Upsert<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Upsert(key, value, 0, expiration,
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(Upsert(key, value, 0, expiration,
                replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, expiration,
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Upsert<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo,
            PersistTo persistTo)
        {
            return Upsert(key, value, cas, expiration,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Upsert(key, value, cas, expiration,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> UpsertAsync<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Upsert(key, value, 0, expiration,
                    replicateTo, persistTo, timeout));
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items)
        {
            return Upsert(items, new ParallelOptions(), 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items, TimeSpan timeout)
        {
            return Upsert(items, new ParallelOptions(), 30, timeout);
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items, ParallelOptions options)
        {
            return Upsert(items, options, 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items, ParallelOptions options, TimeSpan timeout)
        {
            return Upsert(items, options, 30, timeout);
        }

        public IDictionary<string, IOperationResult<T>> Upsert<T>(IDictionary<string, T> items, ParallelOptions options, int rangeSize)
        {
            return Upsert(items, options, rangeSize, DefaultOperationTimeout);
        }

        #endregion

        #region Replace

        public IDocumentResult<T> Replace<T>(IDocument<T> document)
        {
            return Replace(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Replace<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Replace(document, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document)
        {
            return Task.FromResult(Replace(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Task.FromResult(Replace(document, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Replace<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Replace(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Replace<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Replace(document, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Task.FromResult(Replace(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(Replace(document, replicateTo, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Replace<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Replace(document, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(Replace(document, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> ReplaceAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(Replace(document, replicateTo, persistTo, timeout));
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents)
        {
            return ReplaceAsync(documents, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents, TimeSpan timeout)
        {
            return ReplaceAsync(documents, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo)
        {
            return ReplaceAsync(documents, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return ReplaceAsync(documents, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> ReplaceAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return ReplaceAsync(documents, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value)
        {
            return Replace(key, value, 0, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.Zero,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, uint expiration)
        {
            return Replace(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Replace(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, uint expiration)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, TimeSpan expiration)
        {
            return Replace(key, value, 0, expiration,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Replace(key, value, 0, expiration,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, TimeSpan expiration)
        {
            return Task.FromResult(
                Replace(key, value, 0, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, 0, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.Zero,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, uint expiration)
        {
            return Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, uint expiration, TimeSpan timeout)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, uint expiration)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, uint expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, TimeSpan expiration)
        {
            return Replace(key, value, cas, expiration,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, TimeSpan expiration, TimeSpan timeout)
        {
            return Replace(key, value, cas, expiration,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, TimeSpan expiration)
        {
            return Task.FromResult(
                Replace(key, value, cas, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, cas, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Replace(key, value, 0, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Replace(key, value, 0, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, ReplicateTo replicateTo)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Replace(key, value, 0, TimeSpan.Zero,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Replace(key, value, 0, TimeSpan.Zero,
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.Zero,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, 0, TimeSpan.Zero,
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo,
            PersistTo persistTo)
        {
            return Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo,
            PersistTo persistTo, TimeSpan timeout)
        {
            return Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Replace(key, value, cas, TimeSpan.Zero,
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(Replace(key, value, cas, TimeSpan.Zero,
                replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, cas, TimeSpan.Zero,
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Replace<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo,
            PersistTo persistTo)
        {
            return Replace(key, value, cas, expiration,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Replace(key, value, cas, expiration,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> ReplaceAsync<T>(string key, T value, ulong cas, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Replace(key, value, 0, expiration,
                    replicateTo, persistTo, timeout));
        }

        #endregion

        #region Insert

        public IDocumentResult<T> Insert<T>(IDocument<T> document)
        {
            return Insert(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Insert<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Insert(document, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document)
        {
            return Task.FromResult(Insert(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Task.FromResult(Insert(document, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Insert<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Insert(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IDocumentResult<T> Insert<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Insert(document, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Task.FromResult(Insert(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(Insert(document, replicateTo, PersistTo.Zero, timeout));
        }

        public IDocumentResult<T> Insert<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Insert(document, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(Insert(document, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> InsertAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(Insert(document, replicateTo, persistTo, timeout));
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents)
        {
            return InsertAsync(documents, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents, TimeSpan timeout)
        {
            return InsertAsync(documents, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo)
        {
            return InsertAsync(documents, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return InsertAsync(documents, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IDocumentResult<T>[]> InsertAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return InsertAsync(documents, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value)
        {
            return Insert(key, value, TimeSpan.Zero,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.Zero,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, uint expiration)
        {
            return Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, uint expiration)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, uint expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, TimeSpan expiration)
        {
            return Insert(key, value, expiration,
                ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Insert(key, value, expiration,
                ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, TimeSpan expiration)
        {
            return Task.FromResult(
                Insert(key, value, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, expiration,
                    ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Insert(key, value, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Insert(key, value, TimeSpan.Zero,
                replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.Zero,
                    replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Insert(key, value, TimeSpan.Zero,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Insert(key, value, TimeSpan.Zero,
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.Zero,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.Zero,
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult<T> Insert<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, uint expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, TimeSpan.FromMilliseconds(expiration),
                    replicateTo, persistTo, timeout));
        }

        public IOperationResult<T> Insert<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Insert(key, value, expiration,
                replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Insert(key, value, expiration,
                    replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> InsertAsync<T>(string key, T value, TimeSpan expiration, ReplicateTo replicateTo, PersistTo persistTo,
            TimeSpan timeout)
        {
            return Task.FromResult(
                Insert(key, value, expiration,
                    replicateTo, persistTo, timeout));
        }

        #endregion

        #region Remove

        public IOperationResult Remove<T>(IDocument<T> document)
        {
            return Remove(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Remove(document, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document)
        {
            return Task.FromResult(
                Remove(document, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(document, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Remove(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Remove(document, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Remove(document, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(document, replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Remove(document, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Remove(document, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync<T>(IDocument<T> document, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(document, replicateTo, persistTo, timeout));
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents)
        {
            return RemoveAsync(documents, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents, TimeSpan timeout)
        {
            return RemoveAsync(documents, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo)
        {
            return RemoveAsync(documents, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return RemoveAsync(documents, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult[]> RemoveAsync<T>(List<IDocument<T>> documents, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return RemoveAsync(documents, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key)
        {
            return Remove(key, 0, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key, TimeSpan timeout)
        {
            return Remove(key, 0, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync(string key)
        {
            return Task.FromResult(
                Remove(key, 0, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, 0, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove(string key, ulong cas)
        {
            return Remove(key, cas, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key, ulong cas, TimeSpan timeout)
        {
            return Remove(key, cas, ReplicateTo.Zero, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas)
        {
            return Task.FromResult(
                Remove(key, cas, ReplicateTo.Zero, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, cas, ReplicateTo.Zero, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove(string key, ReplicateTo replicateTo)
        {
            return Remove(key, 0, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Remove(key, 0, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync(string key, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Remove(key, 0, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, 0, replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove(string key, ulong cas, ReplicateTo replicateTo)
        {
            return Remove(key, cas, replicateTo, PersistTo.Zero, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key, ulong cas, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Remove(key, cas, replicateTo, PersistTo.Zero, timeout);
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas, ReplicateTo replicateTo)
        {
            return Task.FromResult(
                Remove(key, cas, replicateTo, PersistTo.Zero, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas, ReplicateTo replicateTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, cas, replicateTo, PersistTo.Zero, timeout));
        }

        public IOperationResult Remove(string key, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Remove(key, 0, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public IOperationResult Remove(string key, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Remove(key, 0, replicateTo, persistTo, timeout);
        }

        public Task<IOperationResult> RemoveAsync(string key, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Remove(key, 0, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, 0, replicateTo, persistTo, timeout));
        }

        public IOperationResult Remove(string key, ulong cas, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Remove(key, cas, replicateTo, persistTo, DefaultOperationTimeout);
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas, ReplicateTo replicateTo, PersistTo persistTo)
        {
            return Task.FromResult(
                Remove(key, cas, replicateTo, persistTo, DefaultOperationTimeout));
        }

        public Task<IOperationResult> RemoveAsync(string key, ulong cas, ReplicateTo replicateTo, PersistTo persistTo, TimeSpan timeout)
        {
            return Task.FromResult(
                Remove(key, 0, replicateTo, persistTo, timeout));
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys)
        {
            return Remove(keys, new ParallelOptions(), 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys, TimeSpan timeout)
        {
            return Remove(keys, new ParallelOptions(), 30, timeout);
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys, ParallelOptions options)
        {
            return Remove(keys, options, 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys, ParallelOptions options, TimeSpan timeout)
        {
            return Remove(keys, options, 30, timeout);
        }

        public IDictionary<string, IOperationResult> Remove(IList<string> keys, ParallelOptions options, int rangeSize)
        {
            return Remove(keys, options, rangeSize, DefaultOperationTimeout);
        }

        #endregion

        #region Touch

        public IOperationResult Touch(string key, TimeSpan expiration)
        {
            return Touch(key, expiration, DefaultOperationTimeout);
        }

        public Task<IOperationResult> TouchAsync(string key, TimeSpan expiration)
        {
            return Task.FromResult(Touch(key, expiration, DefaultOperationTimeout));
        }

        public Task<IOperationResult> TouchAsync(string key, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(Touch(key, expiration, timeout));
        }

        #endregion

        #region GetAndTouch

        public IOperationResult<T> GetAndTouch<T>(string key, TimeSpan expiration)
        {
            return GetAndTouch<T>(key, expiration, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> GetAndTouchAsync<T>(string key, TimeSpan expiration)
        {
            return Task.FromResult(GetAndTouch<T>(key, expiration, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> GetAndTouchAsync<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(GetAndTouch<T>(key, expiration, timeout));
        }

        public IDocumentResult<T> GetAndTouchDocument<T>(string key, TimeSpan expiration)
        {
            return GetAndTouchDocument<T>(key, expiration, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> GetAndTouchDocumentAsync<T>(string key, TimeSpan expiration)
        {
            return Task.FromResult(GetAndTouchDocument<T>(key, expiration, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> GetAndTouchDocumentAsync<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            return Task.FromResult(GetAndTouchDocument<T>(key, expiration, timeout));
        }

        #endregion

        #region Get

        public IDocumentResult<T> GetDocument<T>(string id)
        {
            return GetDocument<T>(id, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> GetDocumentAsync<T>(string id)
        {
            return Task.FromResult(GetDocument<T>(id, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> GetDocumentAsync<T>(string id, TimeSpan timeout)
        {
            return Task.FromResult(GetDocument<T>(id, timeout));
        }

        public Task<IDocumentResult<T>[]> GetDocumentsAsync<T>(IEnumerable<string> ids)
        {
            return GetDocumentsAsync<T>(ids, DefaultOperationTimeout);
        }

        public IOperationResult<T> Get<T>(string key)
        {
            return Get<T>(key, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> GetAsync<T>(string key)
        {
            return Task.FromResult(Get<T>(key, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> GetAsync<T>(string key, TimeSpan timeout)
        {
            return Task.FromResult(Get<T>(key, timeout));
        }

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys)
        {
            return Get<T>(keys, new ParallelOptions(), 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys, TimeSpan timeout)
        {
            return Get<T>(keys, new ParallelOptions(), 30, timeout);
        }

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys, ParallelOptions options)
        {
            return Get<T>(keys, options, 30, DefaultOperationTimeout);
        }

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys, ParallelOptions options, TimeSpan timeout)
        {
            return Get<T>(keys, options, 30, timeout);
        }

        public IDictionary<string, IOperationResult<T>> Get<T>(IList<string> keys, ParallelOptions options, int rangeSize)
        {
            return Get<T>(keys, options, rangeSize, DefaultOperationTimeout);
        }

        #endregion

        #region GetFromReplica

        public IDocumentResult<T> GetDocumentFromReplica<T>(string id)
        {
            return GetDocumentFromReplica<T>(id, DefaultOperationTimeout);
        }

        public Task<IDocumentResult<T>> GetDocumentFromReplicaAsync<T>(string id)
        {
            return Task.FromResult(GetDocumentFromReplica<T>(id, DefaultOperationTimeout));
        }

        public Task<IDocumentResult<T>> GetDocumentFromReplicaAsync<T>(string id, TimeSpan timeout)
        {
            return Task.FromResult(GetDocumentFromReplica<T>(id, timeout));
        }

        public IOperationResult<T> GetFromReplica<T>(string key)
        {
            return GetFromReplica<T>(key, DefaultOperationTimeout);
        }

        public Task<IOperationResult<T>> GetFromReplicaAsync<T>(string key)
        {
            return Task.FromResult(GetFromReplica<T>(key, DefaultOperationTimeout));
        }

        public Task<IOperationResult<T>> GetFromReplicaAsync<T>(string key, TimeSpan timeout)
        {
            return Task.FromResult(GetFromReplica<T>(key, timeout));
        }

        #endregion

        #region GetWithLock

        public IOperationResult<T> GetWithLock<T>(string key, uint expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<T> GetAndLock<T>(string key, uint expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<T> GetAndLock<T>(string key, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetWithLockAsync<T>(string key, uint expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetAndLockAsync<T>(string key, uint expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetAndLockAsync<T>(string key, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<T> GetWithLock<T>(string key, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<T> GetAndLock<T>(string key, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<T> GetAndLock<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetWithLockAsync<T>(string key, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetAndLockAsync<T>(string key, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<T>> GetAndLockAsync<T>(string key, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Unlock

        public IOperationResult Unlock(string key, ulong cas)
        {
            throw new NotImplementedException();
        }

        public IOperationResult Unlock(string key, ulong cas, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult> UnlockAsync(string key, ulong cas)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult> UnlockAsync(string key, ulong cas, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Increment

        public IOperationResult<ulong> Increment(string key)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, ulong initial)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, ulong initial)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, ulong initial, uint expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, ulong initial, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, ulong initial, uint expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, ulong initial, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, ulong initial, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Increment(string key, ulong delta, ulong initial, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, ulong initial, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> IncrementAsync(string key, ulong delta, ulong initial, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Decrement

        public IOperationResult<ulong> Decrement(string key)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, ulong initial)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, ulong initial)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, ulong initial, uint expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, ulong initial, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, ulong initial, uint expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, ulong initial, uint expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, ulong initial, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<ulong> Decrement(string key, ulong delta, ulong initial, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, ulong initial, TimeSpan expiration)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<ulong>> DecrementAsync(string key, ulong delta, ulong initial, TimeSpan expiration, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Append

        public IOperationResult<string> Append(string key, string value)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<string> Append(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<string>> AppendAsync(string key, string value)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<string>> AppendAsync(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<byte[]> Append(string key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<byte[]> Append(string key, byte[] value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<byte[]>> AppendAsync(string key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<byte[]>> AppendAsync(string key, byte[] value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Prepend

        public IOperationResult<string> Prepend(string key, string value)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<string> Prepend(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<string>> PrependAsync(string key, string value)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<string>> PrependAsync(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<byte[]> Prepend(string key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public IOperationResult<byte[]> Prepend(string key, byte[] value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<byte[]>> PrependAsync(string key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public Task<IOperationResult<byte[]>> PrependAsync(string key, byte[] value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Query

        public IViewResult<T> Query<T>(IViewQueryable query)
        {
            throw new NotImplementedException();
        }

        public Task<IViewResult<T>> QueryAsync<T>(IViewQueryable query)
        {
            throw new NotImplementedException();
        }

        public IQueryResult<T> Query<T>(string query)
        {
            throw new NotImplementedException();
        }

        public Task<IQueryResult<T>> QueryAsync<T>(string query)
        {
            throw new NotImplementedException();
        }

        public IQueryResult<T> Query<T>(IQueryRequest queryRequest)
        {
            throw new NotImplementedException();
        }

        public Task<IQueryResult<T>> QueryAsync<T>(IQueryRequest queryRequest)
        {
            throw new NotImplementedException();
        }

        public Task<IQueryResult<T>> QueryAsync<T>(IQueryRequest queryRequest, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public IAnalyticsResult<T> Query<T>(IAnalyticsRequest analyticsRequest)
        {
            throw new NotImplementedException();
        }

        public Task<IAnalyticsResult<T>> QueryAsync<T>(IAnalyticsRequest analyticsRequest)
        {
            throw new NotImplementedException();
        }

        public Task<IAnalyticsResult<T>> QueryAsync<T>(IAnalyticsRequest analyticsRequest, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public ISearchQueryResult Query(SearchQuery searchQuery)
        {
            throw new NotImplementedException();
        }

        public Task<ISearchQueryResult> QueryAsync(SearchQuery searchQuery)
        {
            throw new NotImplementedException();
        }

        public Task<ISearchQueryResult> QueryAsync(SearchQuery searchQuery, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region CreateQuery

        public IViewQuery CreateQuery(string designDoc, string view)
        {
            throw new NotImplementedException();
        }

        public IViewQuery CreateQuery(string designdoc, string view, bool development)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region MutateIn

        public IMutateInBuilder<TDocument> MutateIn<TDocument>(string key)
        {
            throw new NotImplementedException();
        }

        public IMutateInBuilder<TDocument> MutateIn<TDocument>(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region LookupIn

        public ILookupInBuilder<TDocument> LookupIn<TDocument>(string key)
        {
            throw new NotImplementedException();
        }

        public ILookupInBuilder<TDocument> LookupIn<TDocument>(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region MapGet

        public IResult<TContent> MapGet<TContent>(string key, string mapkey)
        {
            throw new NotImplementedException();
        }

        public IResult<TContent> MapGet<TContent>(string key, string mapkey, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<TContent>> MapGetAsync<TContent>(string key, string mapkey)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<TContent>> MapGetAsync<TContent>(string key, string mapkey, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region MapRemove

        public IResult MapRemove(string key, string mapkey)
        {
            throw new NotImplementedException();
        }

        public IResult MapRemove(string key, string mapkey, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> MapRemoveAsync(string key, string mapkey)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> MapRemoveAsync(string key, string mapkey, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region MapSize

        public IResult<int> MapSize(string key)
        {
            throw new NotImplementedException();
        }

        public IResult<int> MapSize(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> MapSizeAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> MapSizeAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region MapAdd

        public IResult MapAdd(string key, string mapkey, string value, bool createMap)
        {
            throw new NotImplementedException();
        }

        public IResult MapAdd(string key, string mapkey, string value, bool createMap, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> MapAddAsync(string key, string mapkey, string value, bool createMap)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> MapAddAsync(string key, string mapkey, string value, bool createMap, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListGet

        public IResult<TContent> ListGet<TContent>(string key, int index)
        {
            throw new NotImplementedException();
        }

        public IResult<TContent> ListGet<TContent>(string key, int index, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<TContent>> ListGetAsync<TContent>(string key, int index)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<TContent>> ListGetAsync<TContent>(string key, int index, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListAppend

        public IResult ListAppend(string key, object value, bool createList)
        {
            throw new NotImplementedException();
        }

        public IResult ListAppend(string key, object value, bool createList, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListAppendAsync(string key, object value, bool createList)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListAppendAsync(string key, object value, bool createList, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListPrepend

        public IResult ListPrepend(string key, object value, bool createList)
        {
            throw new NotImplementedException();
        }

        public IResult ListPrepend(string key, object value, bool createList, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListPrependAsync(string key, object value, bool createList)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListPrependAsync(string key, object value, bool createList, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListRemove

        public IResult ListRemove(string key, int index)
        {
            throw new NotImplementedException();
        }

        public IResult ListRemove(string key, int index, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListRemoveAsync(string key, int index)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListRemoveAsync(string key, int index, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListSet

        public IResult ListSet(string key, int index, string value)
        {
            throw new NotImplementedException();
        }

        public IResult ListSet(string key, int index, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListSetAsync(string key, int index, string value)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> ListSetAsync(string key, int index, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region ListSize

        public IResult<int> ListSize(string key)
        {
            throw new NotImplementedException();
        }

        public IResult<int> ListSize(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> ListSizeAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> ListSizeAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region SetAdd

        public IResult SetAdd(string key, string value, bool createSet)
        {
            throw new NotImplementedException();
        }

        public IResult SetAdd(string key, string value, bool createSet, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> SetAddAsync(string key, string value, bool createSet)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> SetAddAsync(string key, string value, bool createSet, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region SetContains

        public IResult<bool> SetContains(string key, string value)
        {
            throw new NotImplementedException();
        }

        public IResult<bool> SetContains(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<bool>> SetContainsAsync(string key, string value)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<bool>> SetContainsAsync(string key, string value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region SetSize

        public IResult<int> SetSize(string key)
        {
            throw new NotImplementedException();
        }

        public IResult<int> SetSize(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> SetSizeAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> SetSizeAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region SetRemove

        public IResult SetRemove<T>(string key, T value)
        {
            throw new NotImplementedException();
        }

        public IResult SetRemove<T>(string key, T value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> SetRemoveAsync<T>(string key, T value)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> SetRemoveAsync<T>(string key, T value, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region QueuePush

        public IResult QueuePush<T>(string key, T value, bool createQueue)
        {
            throw new NotImplementedException();
        }

        public IResult QueuePush<T>(string key, T value, bool createQueue, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> QueuePushAsync<T>(string key, T value, bool createQueue)
        {
            throw new NotImplementedException();
        }

        public Task<IResult> QueuePushAsync<T>(string key, T value, bool createQueue, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region QueuePop

        public IResult<T> QueuePop<T>(string key)
        {
            throw new NotImplementedException();
        }

        public IResult<T> QueuePop<T>(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<T>> QueuePopAsync<T>(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<T>> QueuePopAsync<T>(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region QueueSize

        public IResult<int> QueueSize(string key)
        {
            throw new NotImplementedException();
        }

        public IResult<int> QueueSize(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> QueueSizeAsync(string key)
        {
            throw new NotImplementedException();
        }

        public Task<IResult<int>> QueueSizeAsync(string key, TimeSpan timeout)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region CreateManager

        public IBucketManager CreateManager(string username, string password)
        {
            throw new NotImplementedException();
        }

        public IBucketManager CreateManager()
        {
            throw new NotImplementedException();
        }

        #endregion

        #region GetClusterVersion

        public ClusterVersion? GetClusterVersion()
        {
            return ClusterVersion;
        }

        public Task<ClusterVersion?> GetClusterVersionAsync()
        {
            return Task.FromResult<ClusterVersion?>(ClusterVersion);
        }

        #endregion

        #region Ping

        public IPingReport Ping(params ServiceType[] services)
        {
            throw new NotImplementedException();
        }

        public IPingReport Ping(string reportId, params ServiceType[] services)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Helpers

        private static Durability GetSuccessfulDurability(ReplicateTo replicateTo, PersistTo persistTo) =>
            replicateTo != ReplicateTo.Zero || persistTo != PersistTo.Zero
                ? Durability.Satisfied
                : Durability.Unspecified;

        private DateTime? GetNewExpiration(TimeSpan expiration) =>
            expiration > TimeSpan.Zero
                ? _clockProvider.GetCurrentDateTime().Add(expiration)
                : (DateTime?) null;

        private T AddOrUpdate<T>(
            string key,
            MockDocument value,
            Func<T> notFound,
            Func<MockDocument, T> found)
        where T : IOperationResult
        {
            var result = default(T);

            _bucket.AddOrUpdate(key,
                _ =>
                {
                    result = notFound();

                    if (result.Success)
                    {
                        return value;
                    }
                    else
                    {
                        return null;
                    }
                },
                (_, currentValue) =>
                {
                    if (currentValue == null ||
                        currentValue.IsExpired(_clockProvider.GetCurrentDateTime()))
                    {
                        result = notFound();
                    }
                    else
                    {
                        result = found(currentValue);
                    }

                    if (result.Success)
                    {
                        return value;
                    }
                    else
                    {
                        return currentValue;
                    }
                });

            return result;
        }

        private T InternalGet<T>(
            string key,
            Func<MockDocument, T> found,
            Func<T> notFound)
        {
            if (_bucket.TryGetValue(key, out var currentValue))
            {
                var now = _clockProvider.GetCurrentDateTime();

                if (currentValue != null && !currentValue.IsExpired(now))
                {
                    return found(currentValue);
                }
            }

            return notFound();
        }

        #endregion
    }
}

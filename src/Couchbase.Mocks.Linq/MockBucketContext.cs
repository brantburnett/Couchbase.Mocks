using System;
using System.Linq;
using Couchbase.Configuration.Client;
using Couchbase.Core;
using Couchbase.Linq;
using Couchbase.Linq.Filters;
using Couchbase.N1QL;

namespace Couchbase.Mocks.Linq
{
    public class MockBucketContext : IBucketContext
    {
        private readonly MockBucket _bucket;
        private MutationState _mutationState;

        public string BucketName => _bucket.Name;
        public IBucket Bucket => _bucket;
        public ClientConfiguration Configuration => _bucket.Cluster?.Configuration;
        public bool ChangeTrackingEnabled => false;
        public MutationState MutationState => _mutationState;

        #region Constructors

        public MockBucketContext(MockBucket bucket)
        {
            _bucket = bucket ?? throw new ArgumentNullException(nameof(bucket));

            ResetMutationState();
        }

        #endregion

        #region Query

        public IQueryable<T> Query<T>()
        {
            return Query<T>(BucketQueryOptions.None);
        }

        public IQueryable<T> Query<T>(BucketQueryOptions options)
        {
            var query = _bucket.Select(p => p.Value?.Content).OfType<T>().AsQueryable();

            if ((options & BucketQueryOptions.SuppressFilters) == BucketQueryOptions.None)
            {
                query = DocumentFilterManager.ApplyFilters(query);
            }

            return query;
        }

        #endregion

        #region Change Tracking

        public void BeginChangeTracking()
        {
            throw new NotImplementedException();
        }

        public void EndChangeTracking()
        {
            throw new NotImplementedException();
        }

        public void SubmitChanges()
        {
            throw new NotImplementedException();
        }

        public void SubmitChanges(SaveOptions options)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Mutations

        public void Save<T>(T document)
        {
            throw new NotImplementedException();
        }

        public void Remove<T>(T document)
        {
            throw new NotImplementedException();
        }

        public void ResetMutationState()
        {
            _mutationState = new MutationState();;
        }

        #endregion
    }
}

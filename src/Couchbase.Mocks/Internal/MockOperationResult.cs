using System;
using Couchbase.Core.Buckets;
using Couchbase.IO;
using Couchbase.IO.Operations;

namespace Couchbase.Mocks.Internal
{
    public class MockOperationResult : IOperationResult
    {
        public ulong Cas { get; set; }
        public Durability Durability { get; set; }
        public Exception Exception { get; set; }
        public string Id { get; set; }
        public string Message { get; set; }
        public OperationCode OpCode { get; set; }
        public ResponseStatus Status { get; set; }
        public bool Success { get; set; }
        public MutationToken Token { get; set; }

        public bool ShouldRetry()
        {
            return false;
        }

        public bool IsNmv()
        {
            return false;
        }
    }
}

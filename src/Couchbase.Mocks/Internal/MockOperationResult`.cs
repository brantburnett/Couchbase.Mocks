using System;

namespace Couchbase.Mocks.Internal
{
    internal class MockOperationResult<T> :
        MockOperationResult, IOperationResult<T>
    {
        public T Value { get; set; }
    }
}

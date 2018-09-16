using System;

namespace Couchbase.Mocks
{
    public class MockDocument
    {
        public DateTime? Expiration { get; set; }
        public object Content { get; set; }
        public ulong Cas { get; set; }

        public bool IsExpired(DateTime now)
        {
            return Expiration != null && now > Expiration;
        }
    }
}

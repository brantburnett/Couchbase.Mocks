using System;

namespace Couchbase.Mocks
{
    public interface IClockProvider
    {
        DateTime GetCurrentDateTime();
    }
}

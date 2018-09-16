using System;

namespace Couchbase.Mocks
{
    public class LambdaClockProvider : IClockProvider
    {
        private readonly Func<DateTime> _lambda;

        public LambdaClockProvider(Func<DateTime> lambda)
        {
            _lambda = lambda ?? throw new ArgumentNullException(nameof(lambda));
        }

        public DateTime GetCurrentDateTime()
        {
            return _lambda();
        }
    }
}

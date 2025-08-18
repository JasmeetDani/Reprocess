using Microsoft.Extensions.Logging;

namespace DLQReprocessing
{
    public class CustomLoggerFactory
    {
        public CustomLogger Create(string correlationId, ILogger<CustomLogger> logger)
        {
            return new CustomLogger(correlationId, logger);
        }
    }
}
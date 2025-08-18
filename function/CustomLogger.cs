using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace DLQReprocessing
{
    public class CustomLogger
    {
        private readonly string correlationId;
        private readonly ILogger<CustomLogger> logger;

        public CustomLogger(string correlationId, ILogger<CustomLogger> logger)
        {
            this.correlationId = correlationId;
            this.logger = logger;
        }

        public void Info(string message)
        {
            // Create a JSON object with the message and correlation ID
            var jsonObject = new
            {
                message = message,
                correlation_id = this.correlationId
            };

            // Convert the JSON object to a string
            string jsonString = JsonSerializer.Serialize(jsonObject);

            // Log the JSON string at the info level
            logger.LogInformation(jsonString);
        }

        public void Error(string message)
        {
            // Create a JSON object with the message and correlation ID
            var jsonObject = new
            {
                message = message,
                correlation_id = this.correlationId
            };

            // Convert the JSON object to a string
            string jsonString = JsonSerializer.Serialize(jsonObject);

            // Log the JSON string at the error level
            logger.LogError(jsonString);
        }
    }
}
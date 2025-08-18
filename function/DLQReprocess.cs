using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net;
using System.Text;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace DLQReprocessing
{
    public class DLQReprocess
    {
        private readonly CustomLoggerFactory _loggerFactory;
        private readonly ILogger<CustomLogger> _logger;

        public DLQReprocess(CustomLoggerFactory loggerFactory, ILogger<CustomLogger> logger)
        {
            _loggerFactory = loggerFactory;
            _logger = logger;
        }

        [Function("DLQReprocess")]
        public async Task<IActionResult> RunAsync([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequest req)
        {
            IServiceBusClient? reprocessor = null;
            IntegrationsDLQ? integrations_dlq = null;

            var correlation_id = req.Headers["correlation_id"];

            var _log = _loggerFactory.Create(correlation_id, _logger);
            _log.Info("DLQReprocess function processed a request.");

            try
            {
                // Function validate_params checks for required parameters and returns dictionary object if all mandatory parameters are available. 
                // If any parameter is missing, it prepares BadRequest HttpResponse object (line# 112) and returns it.
                var parameters = ValidateParams(req, _log, correlation_id);

                // if validate_params returns HttpResponse object, it means some parameter is missing. So, return the same HttpResponse object.

                if (parameters.GetType() == typeof(HttpResponseMessage))
                {
                    return await ((HttpResponseMessage)parameters).ToObjectResult();
                }

                _log.Info($"Request parameters - {parameters}");

                string? sb_namespace = Environment.GetEnvironmentVariable("ServiceBusFullyQualifiedNamespace");

                string? global_dlq_name = Environment.GetEnvironmentVariable("IntegrationsDlqName");
                long batch_size = Convert.ToInt64((Environment.GetEnvironmentVariable("DefaultBatchSize")));
                int max_retry_count = parameters.max_retry_count != null ? Convert.ToInt32(parameters.max_retry_count) : Convert.ToInt32(Environment.GetEnvironmentVariable("DefaultMaxRetryCount"));

                if (parameters.subscription_name == null)
                {
                    reprocessor = new Queue(parameters.queue_topic_name, sb_namespace);
                }
                else
                {
                    reprocessor = new TopicSubscription(parameters.queue_topic_name, parameters.subscription_name, sb_namespace);
                }

                long deadLetterCount = await reprocessor.GetDlqMessageCount();
                _log.Info($"Total number of messages found in DLQ = {deadLetterCount}");

                if (deadLetterCount == 0)
                {
                    return await PrepareJsonResponse(200, "No dead letter messages to process", correlation_id).ToObjectResult();
                }

                var batch_count = Math.Ceiling((double)deadLetterCount / batch_size);

                var remaining_msg_count = deadLetterCount;

                integrations_dlq = new IntegrationsDLQ(global_dlq_name, sb_namespace);
                var retry_exhausted_message_list = new List<string>();
                var retryMessageCount = 0;

                _log.Info($"Processing {deadLetterCount} messages with batch size of {batch_size}");

                for (int i = 1; i <= batch_count; ++i)
                {
                    long currentBatchSize = remaining_msg_count < batch_size ? remaining_msg_count : batch_size;

                    // Receive and process messages from the dead letter queue
                    Tuple<long, int> ret = await ProcessBatch(reprocessor, integrations_dlq, parameters.dead_letter_reason, max_retry_count, remaining_msg_count, retry_exhausted_message_list, currentBatchSize, retryMessageCount);
                    remaining_msg_count = ret.Item1;
                    retryMessageCount = ret.Item2;
                }

                var integration_dlq_msg_count = deadLetterCount - retryMessageCount;
                var msg = $"Processed {deadLetterCount} dead letter messages. {integration_dlq_msg_count} messages sent to integrations-dlq and {retryMessageCount} messages retried to original topic/queue.";
                _log.Info(msg);

                return await PrepareJsonResponse(200, msg, correlation_id, retry_exhausted_message_list).ToObjectResult();
            }
            catch (Exception e)
            {
                var msg = $"Function execution failed.Error message - {e.Message}";
                _log.Error(msg);
                return await PrepareJsonResponse(500, msg, correlation_id).ToObjectResult();
            }
            finally
            {
                // Close the receiver, sender, and client objects
                if (reprocessor != null)
                    await reprocessor.CloseConnections();

                if (integrations_dlq != null)
                    await integrations_dlq.CloseConnections();
            }
        }

        private HttpResponseMessage PrepareJsonResponse(int statusCode, string responseMsg, string correlationId, List<string> retryExhaustedMessageList = null)
        {
            var data = new Dictionary<string, object>
            {
                { "response-message", responseMsg }
            };

            if (retryExhaustedMessageList != null)
            {
                data["retry-exhausted-message-id-list"] = retryExhaustedMessageList;
            }

            var jsonResponse = JsonConvert.SerializeObject(data);

            var httpResponse = new HttpResponseMessage((HttpStatusCode)statusCode)
            {
                Content = new StringContent(jsonResponse, Encoding.UTF8, "application/json")
            };

            httpResponse.Headers.Add("correlation-id", correlationId);

            return httpResponse;
        }

        private dynamic ValidateParams(HttpRequest req, CustomLogger logger, string correlationId)
        {
            try
            {
                string? subscriptionName = req.Query["subscription-name"];
                string? queueTopicName = req.Query["queue-topic-name"];
                string? deadLetterReason = req.Query["deadletter-reason"];
                string? maxRetryCount = req.Query["max-retrycount"]; // Optional

                logger.Info($"Validating params subscription-name:{subscriptionName},queue-topic-name:{queueTopicName},deadletter-reason:{deadLetterReason},max-retrycount:{maxRetryCount}");

                if (string.IsNullOrEmpty(queueTopicName) || string.IsNullOrEmpty(deadLetterReason))
                {
                    string responseErrorMsg = "Please provide all required parameters: queue-topic-name, subscription-name (if using topic), deadletter-reason.";
                    logger.Error(responseErrorMsg);
                    return PrepareJsonResponse(400, responseErrorMsg, correlationId);
                }

                return new
                {
                    subscription_name = subscriptionName,
                    queue_topic_name = queueTopicName,
                    dead_letter_reason = System.Text.Json.JsonSerializer.Deserialize<List<string>>(deadLetterReason.Replace("'","\"").Replace("\\", "")),
                    max_retry_count = maxRetryCount
                };
            }
            catch (Exception e)
            {
                throw e; // Consider logging the exception here as well
            }
        }

        private async Task<Tuple<long,int>> ProcessBatch(
                IServiceBusClient reprocessor,
                IntegrationsDLQ integrationsDlq,
                List<string> deadLetterReason,
                int maxRetryCount,
                long remainingMsgCount,
                List<string> retryExhaustedMessageList,
                long currentBatchSize,
                int retryMessageCount)
        {
            try
            {
                var msgBatch = await reprocessor.ServiceBusReceiver().ReceiveMessagesAsync((int)currentBatchSize);
                foreach (var msg in msgBatch)
                {
                    // Retrieve message properties
                    int msgRetryCount = msg.ApplicationProperties.ContainsKey("retryCount") ?
                        Convert.ToInt32(msg.ApplicationProperties["retryCount"]) : 0;

                    string msgDeadLetterReason = msg.ApplicationProperties.ContainsKey("DeadLetterReason") ?
                        (msg.ApplicationProperties["DeadLetterReason"]).ToString() : null;

                    var newMsg = new ServiceBusMessage(msg.Body) // Clone existing message
                    {
                        ContentType = msg.ContentType,
                        CorrelationId = msg.CorrelationId,
                        To = msg.To,
                        ReplyTo = msg.ReplyTo,
                        ReplyToSessionId = msg.ReplyToSessionId,
                        SessionId = msg.SessionId,
                        MessageId = msg.MessageId
                    };

                    // Set the subject based on the queue/topic and subscription names, or original subject
                    if (reprocessor is TopicSubscription subscription && !string.IsNullOrEmpty(subscription.SubscriptionName) && !string.IsNullOrEmpty(subscription.TopicName))
                    {
                        newMsg.Subject = msg.Subject;
                    }
                    else if (reprocessor is DLQReprocessing.Queue queue)
                    {
                        newMsg.Subject = msg.Subject;
                    }
                    else if (!string.IsNullOrEmpty(msg.Subject))
                    {
                        newMsg.Subject = msg.Subject;
                    }
                    else
                    {
                        newMsg.Subject = "Unknown";
                    }

                    // Clone ApplicationProperties
                    foreach (var property in msg.ApplicationProperties)
                    {
                        newMsg.ApplicationProperties[property.Key] = property.Value;
                    }

                    if (msgRetryCount < maxRetryCount && deadLetterReason.Contains(msgDeadLetterReason))
                    {
                        subscription = reprocessor as TopicSubscription;

                        // Increment retryCount and send the message back to the original topic
                        newMsg.ApplicationProperties["retryCount"] = msgRetryCount + 1;
                        newMsg.MessageId = Guid.NewGuid().ToString().Replace("-",""); // Generate a new message ID

                        if (subscription != null)
                        {
                            newMsg.ApplicationProperties["integration"] = subscription.SubscriptionName; // For queue, this property will be skipped
                        }

                        await reprocessor.ServiceBusSender().SendMessageAsync(newMsg);
                        retryMessageCount++;
                    }
                    else
                    {
                        // Add description and send to Global DLQ
                        newMsg.ApplicationProperties["description"] = "sent to integrations-dlq from function dlq-reprocessing";
                        if (reprocessor is TopicSubscription)
                        {
                            subscription = reprocessor as TopicSubscription; // Redundant

                            newMsg.ApplicationProperties["source"] = $"{subscription.TopicName} : {subscription.SubscriptionName}";
                        }
                        else
                        {
                            var queue = reprocessor as Queue;

                            newMsg.ApplicationProperties["source"] = queue.QueueName;
                        }

                        await integrationsDlq.ServiceBusSender().SendMessageAsync(newMsg);

                        // Add message id to the list to be returned in response only if retryCount is exceeded
                        if (msgRetryCount >= maxRetryCount)
                        {
                            retryExhaustedMessageList.Add(msg.MessageId);
                        }
                    }

                    await reprocessor.ServiceBusReceiver().CompleteMessageAsync(msg);
                    remainingMsgCount--;
                }

                return new Tuple<long,int>(remainingMsgCount, retryMessageCount);
            }
            catch (Exception ex)
            {
                throw ex; // Consider logging the exception or handling it differently
            }
        }
    }
}
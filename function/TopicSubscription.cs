using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Identity;

namespace DLQReprocessing
{
    public class TopicSubscription : IServiceBusClient
    {
        private string topicName;
        public string TopicName
        {
            get { return topicName; }
        }
        private string subscriptionName;
        public string SubscriptionName
        {
            get { return subscriptionName; }
        }
        private ServiceBusAdministrationClient serviceBusMgmtClient;
        private ServiceBusClient serviceBusClient;
        private ServiceBusReceiver receiver;
        private ServiceBusSender sender;
        private long dlqMsgCount;

        public TopicSubscription(string topicName, string subscriptionName, string serviceBusNamespace)
        {
            this.topicName = topicName;
            this.subscriptionName = subscriptionName;
            this.serviceBusMgmtClient = new ServiceBusAdministrationClient(serviceBusNamespace, new DefaultAzureCredential());
            this.serviceBusClient = new ServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());
            this.receiver = this.serviceBusClient.CreateReceiver(topicName, subscriptionName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter
            });
            // Create a ServiceBusSender object for the original topic
            this.sender = this.serviceBusClient.CreateSender(topicName);
        }

        public async Task<long> GetDlqMessageCount()
        {
            // Get the runtime properties of the subscription
            var subscriptionInfo = await serviceBusMgmtClient.GetSubscriptionRuntimePropertiesAsync(topicName, subscriptionName);
            dlqMsgCount = subscriptionInfo.Value.DeadLetterMessageCount;
            return dlqMsgCount;
        }

        public async Task CloseConnections()
        {
            await receiver.DisposeAsync();
            await sender.DisposeAsync();
            await serviceBusClient.DisposeAsync();
            // await serviceBusMgmtClient.DisposeAsync();
        }

        public ServiceBusReceiver ServiceBusReceiver()
        {
            return receiver;
        }

        public ServiceBusSender ServiceBusSender()
        {
            return sender;
        }
    }
}
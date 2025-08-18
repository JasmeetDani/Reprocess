using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Identity;

namespace DLQReprocessing
{
    public class Queue : IServiceBusClient
    {
        private string queueName;
        public string QueueName
        {
            get { return queueName; }
        }
        private ServiceBusAdministrationClient serviceBusMgmtClient;
        private ServiceBusClient serviceBusClient;
        private ServiceBusReceiver receiver;
        private ServiceBusSender sender;
        private long dlqMsgCount;

        public Queue(string queueName, string serviceBusNamespace)
        {
            this.queueName = queueName;
            this.serviceBusMgmtClient = new ServiceBusAdministrationClient(serviceBusNamespace, new DefaultAzureCredential());
            this.serviceBusClient = new ServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());
            this.receiver = this.serviceBusClient.CreateReceiver(queueName, new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter
            });
            // Create a ServiceBusSender object for the original queue
            this.sender = this.serviceBusClient.CreateSender(queueName);
        }

        public async Task<long> GetDlqMessageCount()
        {
            // Get the runtime properties of the queue
            var queueInfo = await serviceBusMgmtClient.GetQueueRuntimePropertiesAsync(queueName);
            dlqMsgCount = queueInfo.Value.DeadLetterMessageCount;
            return dlqMsgCount;
        }

        public async Task CloseConnections()
        {
            await serviceBusClient.DisposeAsync();
            // await serviceBusMgmtClient.DisposeAsync();
            await receiver.DisposeAsync();
            await sender.DisposeAsync();
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
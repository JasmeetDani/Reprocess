using Azure.Messaging.ServiceBus;
using Azure.Identity;

namespace DLQReprocessing
{
    public class IntegrationsDLQ : IServiceBusClient
    {
        private string queueName;
        private ServiceBusClient serviceBusClient;
        private ServiceBusSender sender;

        public IntegrationsDLQ(string queueName, string serviceBusNamespace)
        {
            this.queueName = queueName;
            this.serviceBusClient = new ServiceBusClient(serviceBusNamespace, new DefaultAzureCredential());
            this.sender = serviceBusClient.CreateSender(queueName);
        }

        public async Task CloseConnections()
        {
            // Close all service bus object connections
            await sender.DisposeAsync();
            await serviceBusClient.DisposeAsync();
        }

        public Task<long> GetDlqMessageCount()
        {
            throw new NotImplementedException();
        }

        public ServiceBusReceiver ServiceBusReceiver()
        {
            throw new NotImplementedException();
        }

        public ServiceBusSender ServiceBusSender()
        {
            return sender;
        }
    }
}
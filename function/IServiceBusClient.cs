using Azure.Messaging.ServiceBus;

namespace DLQReprocessing
{
    public interface IServiceBusClient
    {
        public ServiceBusReceiver ServiceBusReceiver();

        public ServiceBusSender ServiceBusSender();

        public Task<long> GetDlqMessageCount();

        public Task CloseConnections();
    }
}
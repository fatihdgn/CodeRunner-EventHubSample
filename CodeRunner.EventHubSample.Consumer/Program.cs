using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CodeRunner.EventHubSample.Consumer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://peakup-coderunner.servicebus.windows.net/;SharedAccessKeyName=Listener;SharedAccessKey=JvjQ4HxdBux8L7oqrbDWBiBi+5AP7vTLgtwUW8ZpuPc=;EntityPath=events";
        static void Main(string[] args) => MainAsync(args).Wait();

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine("Staring to listen");

            await using var consumer = new EventHubConsumerClient(EventHubConsumerClient.DefaultConsumerGroupName, connectionString);
            CancellationTokenSource source = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            try
            {
                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(source.Token))
                {
                    var message = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
                    Console.WriteLine($"Received message: {message}");
                }
            }
            finally
            {
                Console.WriteLine("Ended listening");
            }
            


        }
    }
}

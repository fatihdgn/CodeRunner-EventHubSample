using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading.Tasks;

namespace CodeRunner.EventHubSample.Producer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://peakup-coderunner.servicebus.windows.net/;SharedAccessKeyName=Sender;SharedAccessKey=TU15ojClsHi8exeLmzepTPlzsZJ0/hWAr0V7P5Cz0E8=;EntityPath=events";
        static void Main(string[] args) => MainAsync(args).Wait();
        static async Task MainAsync(string[] args)
        {
            var message = "Hello";
            await using var producer = new EventHubProducerClient(connectionString);
            Console.WriteLine($"Sending message: {message}");
            using EventDataBatch batch = await producer.CreateBatchAsync();
            batch.TryAdd(new EventData(Encoding.UTF8.GetBytes(message)));

            await producer.SendAsync(batch);
            Console.WriteLine("Message sent.");
        }
    }
}

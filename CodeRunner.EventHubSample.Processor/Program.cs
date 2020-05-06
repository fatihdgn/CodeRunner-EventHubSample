using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Storage.Blobs;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CodeRunner.EventHubSample.Processor
{
    class Program
    {
        static string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=peakupcoderunner;AccountKey=fT5LRN30MWDLoO9aYJvvDUWmdFwS4xty88RjjZsme4PCTtGq+5CTmtntBmR6BWR0AIodp+6NHXHhSv4HhBHYow==;EndpointSuffix=core.windows.net";
        static string storageContainerName = "events2";

        static string connectionString = "Endpoint=sb://peakup-coderunner.servicebus.windows.net/;SharedAccessKeyName=Listener;SharedAccessKey=JvjQ4HxdBux8L7oqrbDWBiBi+5AP7vTLgtwUW8ZpuPc=;EntityPath=events";

        static void Main(string[] args) => MainAsync(args).Wait();

        static async Task MainAsync(string[] args)
        {
            BlobContainerClient storageClient = new BlobContainerClient(storageConnectionString, storageContainerName);
            EventProcessorClient processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, connectionString);
            processor.ProcessEventAsync += Processor_ProcessEventAsync;
            processor.ProcessErrorAsync += Processor_ProcessErrorAsync;

            try
            {
                Console.WriteLine("Starting to listen for 2 minutes.");
                await processor.StartProcessingAsync();
                Console.WriteLine("Started to listen for 2 minutes.");

                await Task.Delay(TimeSpan.FromMinutes(2));
                Console.WriteLine("Stopping the process");
                await processor.StopProcessingAsync();
                Console.WriteLine("Processing stopped");
            }
            finally
            {
                processor.ProcessEventAsync -= Processor_ProcessEventAsync;
                processor.ProcessErrorAsync -= Processor_ProcessErrorAsync;
            }
        }


        private static async System.Threading.Tasks.Task Processor_ProcessEventAsync(Azure.Messaging.EventHubs.Processor.ProcessEventArgs arg)
        {
            if (arg.CancellationToken.IsCancellationRequested) return;

            string message = Encoding.UTF8.GetString(arg.Data.Body.ToArray());
            Console.WriteLine($"Received message: {message}");
            Console.WriteLine($"Creating checkpoint");
            var cancelationSource = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            try
            {
                await arg.UpdateCheckpointAsync(cancelationSource.Token);
                Console.WriteLine($"Checkpoint created!");
            }
            catch (TaskCanceledException)
            {
                Console.WriteLine($"Checkpoint creation took too long and was cancelled.");
            }
        }

        private static System.Threading.Tasks.Task Processor_ProcessErrorAsync(Azure.Messaging.EventHubs.Processor.ProcessErrorEventArgs arg)
        {
            if (arg.CancellationToken.IsCancellationRequested) return Task.CompletedTask;
            Console.WriteLine($"Oops. Operation: {arg.Operation}, Exception message: {arg.Exception.Message}");
            return Task.CompletedTask;
        }

    }
}

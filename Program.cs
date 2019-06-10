using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;

namespace EventStreamer
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint = sb://demoeventhubone.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxxxx";
        private const string EhEntityPath = "demoeventhub";

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        private static async Task MainAsync(string[] args)
        {
            // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
            // Typically, the connection string should have the entity path in it, but for the sake of this simple scenario
            // we are using the connection string from the namespace.
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EhConnectionString)
            {
                EntityPath = EhEntityPath
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            await SendMessagesToEventHub(1000);

            await eventHubClient.CloseAsync();

            Console.WriteLine("Press ENTER to exit.");
            Console.ReadLine();
        }

        // Creates an event hub client and sends 100 messages to the event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            Sales sales = null;
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    sales = new Sales { SalesId=i,  Country = "India", Product = "Car", ProductCategory = "4 Wheelers" };

                    string message = i +","+ sales.SalesId + "," + sales.Country + "," + sales.Product + "," + sales.ProductCategory;
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(10);
            }
            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }
    }

    public class Sales
    {
        public int SalesId { get; set; }
        public string Product { get; set; }
        public string ProductCategory { get; set; }
        public string Country { get; set; }
    }
}


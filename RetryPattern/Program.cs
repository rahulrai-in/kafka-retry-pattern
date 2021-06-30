using System;
using System.Threading.Tasks;

namespace RetryPattern
{
    internal class Program
    {
        private const string BootstrapServer = "127.0.0.1:9092";
        private const string TopicName = "alphabets";

        private static async Task Main()
        {
            Console.WriteLine("Enter processor code \n(1)Producer\n(2)Consumer");
            var selected = Console.ReadKey(false).KeyChar;
            Console.WriteLine("\n");
            switch (selected)
            {
                case '1':
                    var producer = new Producer(BootstrapServer);
                    await producer.StartSendingMessages(TopicName);
                    break;
                case '2':
                    var consumer = new Consumer(BootstrapServer);
                    consumer.StartReceivingMessages(TopicName);
                    break;
            }

            Console.WriteLine("Closing application");
            Console.ReadKey();
        }
    }
}

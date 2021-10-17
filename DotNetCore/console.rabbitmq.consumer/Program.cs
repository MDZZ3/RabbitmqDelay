using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace console.rabbitmq.consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Method2Consumer();
        }

        private static void Method1Consumer()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1"
            };

            //创建连接
            var connection = connectionFactory.CreateConnection();

            var channel = connection.CreateModel();

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);

            consumer.Received += (obj, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine(message);
                channel.BasicAck(ea.DeliveryTag, false);
            };

            channel.BasicConsume("queue.business.test", false, consumer);

            Console.ReadKey();
            channel.Dispose();
            connection.Close();
        }

        private static void Method2Consumer()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1"
            };

            //创建连接
            var connection = connectionFactory.CreateConnection();

            var channel = connection.CreateModel();

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);

            consumer.Received += (obj, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine(message);
                channel.BasicAck(ea.DeliveryTag, false);
            };

            channel.BasicConsume("plug.delay.queue", false, consumer);

            Console.ReadKey();
            channel.Dispose();
            connection.Close();
        }
    }
}

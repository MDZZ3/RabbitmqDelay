using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace console.rabbitmq.production
{
    class Program
    {
        static void Main(string[] args)
        {


            delayMethod2();



        }

        /// <summary>
        /// 利用rabbitmq本身的死信队列实现延时队列
        /// </summary>
        private static void delayMethod1()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1"
            };

            //创建连接
            var connection = connectionFactory.CreateConnection();

            //创建通道
            var channl = connection.CreateModel();

            Dictionary<string, object> queueArgs = new Dictionary<string, object>()
            {
                { "x-dead-letter-exchange","exchange.business.test" },
                {"x-dead-letter-routing-key","businessRoutingkey" }
            };

            //延时的交换机和队列绑定
            channl.ExchangeDeclare("exchange.business.dlx", "direct", true, false, null);
            channl.QueueDeclare("queue.business.dlx", true, false, false, queueArgs);
            channl.QueueBind("queue.business.dlx", "exchange.business.dlx", "");

            //业务的交换机和队列绑定
            channl.ExchangeDeclare("exchange.business.test", "direct", true, false, null);
            channl.QueueDeclare("queue.business.test", true, false, false, null);
            channl.QueueBind("queue.business.test", "exchange.business.test", "businessRoutingkey", null);


            string message1 = "Hello Word!1";
            string message2 = "Hello Word!2";
            var body1 = Encoding.UTF8.GetBytes(message1);
            var body2 = Encoding.UTF8.GetBytes(message2);
            var properties = channl.CreateBasicProperties();
            properties.Persistent = true;
            properties.Expiration = "10000";
            channl.BasicPublish("exchange.business.dlx", "", properties, body2);

            //var properties2 = channl.CreateBasicProperties();
            //properties.Persistent = true;
            //properties.Expiration = "80000";
            properties.Expiration = "20000";
            channl.BasicPublish("exchange.business.dlx", "", properties, body1);
        }

        
        /// <summary>
        /// 利用rabbitmq的延时插件实现
        /// </summary>
        private static void delayMethod2()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1"
            };

            var connection = connectionFactory.CreateConnection();

            var channel = connection.CreateModel();

            Dictionary<string, object> exchangeArgs = new Dictionary<string, object>()
            {
                {"x-delayed-type","direct" }
            };

            channel.ExchangeDeclare("plug.delay.exchange", "x-delayed-message", true, false, exchangeArgs);

            channel.QueueDeclare("plug.delay.queue", true, false, false, null);

            channel.QueueBind("plug.delay.queue", "plug.delay.exchange", "plugdelay");


            string message1 = "Hello Word!1";
            string message2 = "Hello Word!2";
            var body1 = Encoding.UTF8.GetBytes(message1);
            var body2 = Encoding.UTF8.GetBytes(message2);
            var properties = channel.CreateBasicProperties();
            Dictionary<string, object> headers = new Dictionary<string, object>()
            {
                {"x-delay","20000" }
            };

            properties.Persistent = true;
            //properties.Expiration = "10000";
            properties.Headers= headers;

            channel.BasicPublish("plug.delay.exchange", "plugdelay", properties, body2);

            headers["x-delay"]= "10000";
            channel.BasicPublish("plug.delay.exchange", "plugdelay", properties, body1);
        }
    }
}

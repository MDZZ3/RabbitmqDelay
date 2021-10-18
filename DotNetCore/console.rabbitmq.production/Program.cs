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

            #region 控制台输入文字
            //Console.WriteLine("生产者开始发送消息");
            //while (true)
            //{

            //    string message = Console.ReadLine();
            //    var body = Encoding.UTF8.GetBytes(message);
            //    var properties = channl.CreateBasicProperties();
            //    properties.Persistent = true;
            //    properties.Expiration = "5000";
            //    //发送一条延时5秒的消息
            //    channl.BasicPublish("exchange.business.dlx", "", properties, body);

            //}
            #endregion

            #region 缺点展示
            string message1 = "Hello Word!1";
            string message2 = "Hello Word!2";
            var body1 = Encoding.UTF8.GetBytes(message1);
            var body2 = Encoding.UTF8.GetBytes(message2);
            var properties = channl.CreateBasicProperties();    
            properties.Persistent = true;
            properties.Expiration = "20000";
            //先发送延时20秒的消息
            channl.BasicPublish("exchange.business.dlx", "", properties, body2);

            //再发送延时10秒的消息
            properties.Expiration = "10000";
            channl.BasicPublish("exchange.business.dlx", "", properties, body1);
            #endregion
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

            //指定x-delayed-message 类型的交换机，并且添加x-delayed-type属性
            channel.ExchangeDeclare("plug.delay.exchange", "x-delayed-message", true, false, exchangeArgs);

            channel.QueueDeclare("plug.delay.queue", true, false, false, null);

            channel.QueueBind("plug.delay.queue", "plug.delay.exchange", "plugdelay");

            //#region 控制台输入
            //var properties = channel.CreateBasicProperties();
            //Console.WriteLine("生产者开始发送消息");
            //Dictionary<string, object> headers = new Dictionary<string, object>()
            //{
            //    {"x-delay","5000" }
            //};
            //properties.Persistent = true;
            //properties.Headers = headers;
            //while (true)
            //{

            //    string message = Console.ReadLine();
            //    var body = Encoding.UTF8.GetBytes(message);
            //    channel.BasicPublish("plug.delay.exchange", "plugdelay", properties, body);

            //}
            //#endregion

            
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
            properties.Headers = headers;
            //先发送20秒过期的消息
            channel.BasicPublish("plug.delay.exchange", "plugdelay", properties, body2);

            headers["x-delay"] = "10000";
            //再发送10秒过期的消息
            channel.BasicPublish("plug.delay.exchange", "plugdelay", properties, body1);
        }
    }
}

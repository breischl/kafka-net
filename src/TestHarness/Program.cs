﻿using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Collections.Generic;
using System.Text;

namespace TestHarness
{
    class Program
    {
        static void Main(string[] args)
        {
			var options = new KafkaOptions(new Uri("http://CSDKAFKA01:9092"), new Uri("http://CSDKAFKA02:9092"));
            var router = new BrokerRouter(options);
            var client = new Producer(router);

            Task.Factory.StartNew(() =>
                {
                    var consumer = new Consumer(new ConsumerOptions("TestHarness", router));
                    foreach (var data in consumer.Consume())
                    {
                        Console.WriteLine("Response: P{0},O{1} : {2}", data.Meta.PartitionId, data.Meta.Offset, data.Value);
                    }
                });


            Console.WriteLine("Type a message and press enter...");
            while (true)
            {
                var message = Console.ReadLine();
                if (message == "quit") break;
                client.SendMessageAsync("TestHarness", new[] {new Message {Value = Encoding.UTF8.GetBytes(message)}});
            }

            using (client)
            using (router)
            {

            }
        }
    }
}

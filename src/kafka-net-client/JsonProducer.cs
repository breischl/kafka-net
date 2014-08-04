using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using KafkaNet.Common;
using KafkaNet.Model;

namespace KafkaNet.Client
{
    public class JsonProducer
    {
        private readonly Producer _producer;

        public JsonProducer(IBrokerRouter brokerRouter)
        {
            _producer = new Producer(brokerRouter);
        }

        public Task<ProduceResult> Publish<T>(string topic, IEnumerable<T> messages, Int16 acks = 1, int timeoutMS = 1000) where T : class 
        {
            return _producer.SendMessageAsync(topic, ConvertToKafkaMessage(messages));
        }

        private static IEnumerable<Message> ConvertToKafkaMessage<T>(IEnumerable<T> messages) where T : class 
        {
            var hasKey = typeof(T).GetProperty("Key", typeof(string)) != null;

            return messages.Select(m => new Message
                {
                    Key = hasKey ? GetKeyPropertyValue(m).ToUnsizedBytes() : null,
                    Value = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(m))
                });
        }

        private static string GetKeyPropertyValue<T>(T message) where T : class 
        {
            if (message == null) return null;
            var info = message.GetType().GetProperty("Key", typeof(string));

            if (info == null) return null;
            return (string)info.GetValue(message);
        }
    }
}

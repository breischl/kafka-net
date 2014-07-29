using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaNet
{
    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
        public IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs)
        {
            return new KafkaConnection(new KafkaTcpSocket(kafkaAddress), responseTimeoutMs);
        }
    }
}

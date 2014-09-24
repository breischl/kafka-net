using KafkaNet.Model;
using System;

namespace KafkaNet
{
    public interface IKafkaConnectionFactory
    {
        IKafkaConnection Create(KafkaEndpoint endpoint, int responseTimeoutMs);
		IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs);
        KafkaEndpoint Resolve(Uri kafkaAddress);
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using KafkaNet.Common;
using System.Threading.Tasks;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
    public class GzipProducerConsumerTests
    {
        private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);

        private KafkaConnection GetKafkaConnection()
        {
            return new KafkaConnection(new KafkaTcpSocket(new DefaultTraceLog(), _options.KafkaServerUri.First()), _options.ResponseTimeoutMs, _options.Log);
        }

        [Test]
        public async Task EnsureGzipCompressedMessageCanSend()
        {
            //ensure topic exists
            using (var conn = GetKafkaConnection())
            {
                conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic }) }).Wait(TimeSpan.FromSeconds(10));
            }

            using (var router = new BrokerRouter(_options))
            {
                var conn = router.SelectBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);

                var request = new ProduceRequest
                {
                    Acks = 1,
                    TimeoutMS = 1000,
                    Payload = new List<Payload>
                                {
                                    new Payload
                                        {
                                            Codec = MessageCodec.CodecGzip,
                                            Topic = IntegrationConfig.IntegrationCompressionTopic,
                                            Partition = 0,
                                            Messages = new List<Message>
                                                    {
                                                        new Message {Value = 0.ToBytes(), Key = "1".ToBytes()},
                                                        new Message {Value = 1.ToBytes(), Key = "1".ToBytes()},
                                                        new Message {Value = 2.ToBytes(), Key = "1".ToBytes()}
                                                    }
                                        }
                                }
                };

                var response = await conn.Connection.SendAsync(request);
                Assert.That(response.First().Error, Is.EqualTo(0));
            }
        }

		[Test]
		public async Task EnsureGzipCanDecompressMessageFromKafka()
		{
			//TODO: This test requires that there already be published messages (ie, that the previous test has already run)
			//so it can hang indefinitely on the first test run.

			using (var router = new BrokerRouter(_options))
			using (var producer = new Producer(router))
			{

				var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic);

				var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, router),
					offsets.Select(x => new OffsetPosition(x.PartitionId, 0)).ToArray());

				var results = consumer.Consume().Take(3).ToList();

				for (int i = 0; i < 3; i++)
				{
					Assert.That(results[i].Value.ToInt32(), Is.EqualTo(i));
				}
			}
		}
    }
}

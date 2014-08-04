using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using KafkaNet.Common;
using Common.Logging;

namespace kafka_tests.Integration
{
    [TestFixture]
    [Category("Integration")]
	[Timeout(10000)]
    public class ProducerConsumerIntegrationTests
    {
		private static readonly ILog _log = LogManager.GetLogger<ProducerConsumerIntegrationTests>();
		private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);
		private IBrokerRouter _router;

		[TestFixtureSetUp]
		public void FixtureSetup()
		{
			//ensure topic exists
			using (var conn = new KafkaConnection(new KafkaTcpSocket(_options.KafkaServerUri.First()), _options.ResponseTimeoutMs))
			{
				var t1 = conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationCompressionTopic }) });
				var t2 = conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationTopic }) });
				Task.WaitAll(t1, t2);
			}
		}

		[SetUp]
		public void Setup()
		{
			_router = new BrokerRouter(_options);
		}

		[TearDown]
		public void Teardown()
		{
			_router.Dispose();
		}
		
		[Test]
		[TestCase(1)]
		[TestCase(3)]
		[TestCase(10)]
		[TestCase(100)]
		[TestCase(1000)]
        public async Task SendAsyncShouldWork(int amount)
        {
            using (var router = new BrokerRouter(_options))
            {
				var producer = new Producer(new ProducerOptions(router, maxAccumulationTime: TimeSpan.Zero, maxAccumulationMessages: 1000, codec: MessageCodec.CodecNone));
                var tasks = new Task<ProduceResult>[amount];

                for (var i = 0; i < amount; i++)
                {
					tasks[i] = producer.SendMessageAsync(IntegrationConfig.IntegrationTopic,
						new[] { 
							new Message { Key = i.ToBytes(), Value = Guid.NewGuid().ToByteArray() } 
						});
                }

				producer.Flush();

				var results = (await Task.WhenAll(tasks)).ToList();

                Assert.That(results.Count, Is.EqualTo(amount));
				Assert.That(results.All(x => x.FailedMessages.Count == 0));
				Assert.That(results.All(x => x.SuccessfulMessages.Count == 1));
            }
        }

        [Test]
		[Explicit("I think this only works if you're set up for exactly 1 partition.")]
        public async Task ConsumerShouldConsumeInSameOrderAsProduced()
        {
			const int numMessages = 5;
            using (var router = new BrokerRouter(_options))
            {
				var producer = new Producer(router);
                var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);

                using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, router),
                    offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
                {

                    for (int i = 0; i < numMessages; i++)
                    {
                        await producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToBytes(), Key = null } });
                    }

					var results = consumer.Consume().Take(numMessages).ToList();

                    //ensure the produced messages arrived
					_log.InfoFormat("Message order:  {0}", string.Join(", ", results.Select(x => x.Value).ToList()));

					Assert.That(results.Count, Is.EqualTo(numMessages));
					for (int i = 0; i < numMessages; i++)
					{
						Assert.AreEqual(i.ToBytes(), results[i].Value);
					}
                }
            }
        }

		[Test]
		public async Task ConsumerShouldBeAbleToSeekBackToEarlierOffset()
		{
			const int numMessages = 5;
			var producer = new Producer(_router);
			var offsetResponses = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);
			var startOffsets = offsetResponses.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

			using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, _router), startOffsets))
			{
				//Send some messages
				var tasks = Enumerable.Range(0, numMessages).Select(i => producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToBytes(), Key = "1".ToUnsizedBytes() } }));
				Task.WaitAll(tasks.ToArray());

				var results = consumer.Consume().Take(numMessages).ToList();

				//ensure the produced messages arrived
				_log.InfoFormat("Message order: {0}", string.Join(", ", results.Select(x => x.Value).ToList()));

				Assert.That(results.Count, Is.EqualTo(numMessages));
				for (int i = 0; i < numMessages; i++)
				{
					var msg = results.FirstOrDefault(r => r.Value.ToInt32() == i);
					Assert.That(msg, Is.Not.Null);
				}
			}
		}

		[Test]
		[TestCase(MessageCodec.CodecNone)]
		[TestCase(MessageCodec.CodecGzip)]
		public async Task ProducerAndConsumer_WorkTogether(MessageCodec codec)
		{
			const int numMessages = 5;
			var producer = new Producer(new ProducerOptions(_router, codec: codec));
			var offsetResponses = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);
			var startOffsets = offsetResponses.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

			var tasks = Enumerable.Range(0, numMessages).Select(i => producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToBytes(), Key = i.ToBytes() } }));
			var produceResults = (await Task.WhenAll(tasks)).ToList();
			Assert.That(produceResults.All(pr => pr.FailedMessages.Count == 0), "One or more of the produce requests failed");

			using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, _router), startOffsets))
			{
				var results = new List<Message>(5);
				for (int i = 0; i < numMessages; i++)
				{
					var msg = consumer.Consume().First();
					_log.InfoFormat("Received message " + i);
					results.Add(msg);
				}

				//ensure the produced messages arrived
				for (int i = 0; i < numMessages; i++)
				{
					var msg = results.FirstOrDefault(r => r.Value.ToInt32() == i);
					Assert.That(msg, Is.Not.Null);
				}

				//the current offsets should be numMessages positions higher than start
				var currentOffsets = consumer.GetOffsetPosition();
				Assert.That(currentOffsets.Sum(x => x.Offset) - startOffsets.Sum(x => x.Offset), Is.EqualTo(numMessages));
			}
		}

		[Test]
		[Ignore("Hangs consistently and I can't figure out why")]
		public async Task GzipCompressedMessage_CanProduceAndConsume()
		{
			var offsets = await (new MetadataQueries(_router)).GetTopicOffsetAsync(IntegrationConfig.IntegrationCompressionTopic);
			var offsetPositions = offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray();

			var conn = _router.SelectBrokerRoute(IntegrationConfig.IntegrationCompressionTopic, 0);
			var request = new ProduceRequest
			{
				Acks = 1,
				TimeoutMS = 10000,
				Codec = MessageCodec.CodecGzip,
				MessageSets = new List<AnnotatedMessageSet>
                    {
                        new AnnotatedMessageSet
                            {
                                Topic = IntegrationConfig.IntegrationCompressionTopic,
                                Partition = 0,
                                Messages = new List<Message>
								{
									new Message {Value = 0.ToBytes(), Key = 0.ToBytes()},
									new Message {Value = 1.ToBytes(), Key = 1.ToBytes()},
									new Message {Value = 2.ToBytes(), Key = 2.ToBytes()}
								}
                            }
                    }
			};

			var response = await conn.Connection.SendAsync(request);
			Assert.That(response.First().Error, Is.EqualTo(0));

			//Now consume the message we just published
			using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationCompressionTopic, _router), offsetPositions))
			{
				for (int i = 0; i < 3; i++)
				{
					var result = consumer.Consume().First();
					Assert.That(result.Value.ToInt32(), Is.EqualTo(i));
				}
			}
		}

		[Test]
		public async Task ConsumerShouldNotLoseMessageWhenBlocked()
		{
			const int numMessages = 5;
			var testId = Guid.NewGuid();

			var producer = new Producer(_router);
			var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);

			//create consumer with buffer size of 1 (should block upstream)
			using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, _router) { ConsumerBufferSize = 1 },
				offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).ToArray()))
			{

				for (int i = 0; i < numMessages; i++)
				{
					//Intentionally not awaiting this
					producer.SendMessageAsync(IntegrationConfig.IntegrationTopic, new[] { new Message { Value = i.ToBytes(), Key = testId.ToByteArray() } });
				}

				var results = consumer.Consume().Take(numMessages).ToList();
				for (int i = 0; i < numMessages; i++)
				{
					var msg = results.FirstOrDefault(m => m.Value.ToInt32() == i);
					Assert.That(msg, Is.Not.Null);
					Assert.That(new Guid(msg.Key), Is.EqualTo(testId));
					Assert.That(msg.Value.ToInt32(), Is.EqualTo(i));
				}
			}
		}

		[Test]
		public async Task ConsumerShouldMoveToNextAvailableOffsetWhenQueryingForNextMessage()
		{
			var producer = new Producer(_router);

			var offsets = await producer.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);
			Assert.That(offsets.Count, Is.EqualTo(2), "This test requires there to be exactly two paritions.");
			Assert.That(offsets.Count(x => x.Offsets.Max(o => o) > 1000), Is.EqualTo(2), "Need more than 1000 messages in each topic for this test to work.");

			//set offset 1000 messages back on one partition.  We should be able to get all 1000 messages over multiple calls.
			using (var consumer = new Consumer(new ConsumerOptions(IntegrationConfig.IntegrationTopic, _router),
				 offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max() - 1000)).ToArray()))
			{
				var data = consumer.Consume().Take(2000).ToList();

				var consumerOffset = consumer.GetOffsetPosition().OrderBy(x => x.Offset).ToList();
				var serverOffset = offsets.Select(x => new OffsetPosition(x.PartitionId, x.Offsets.Max())).OrderBy(x => x.Offset).ToList();

				Assert.That(consumerOffset, Is.EqualTo(serverOffset), "The consumerOffset position should match the server offset position.");
				Assert.That(data.Count, Is.EqualTo(2000), "We should have received 2000 messages from the server.");

			}
		}
    }
}

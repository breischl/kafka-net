using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using System.Threading.Tasks;
using KafkaNet.Common;
using System.Threading;

namespace kafka_tests.Integration
{
	[TestFixture]
	[Category("Integration")]
	public class ProduceTests
	{
		private readonly KafkaOptions _options = new KafkaOptions(IntegrationConfig.IntegrationUri);
		BrokerRouter _router;

		[SetUp]
		public void Setup()
		{
			_router = new BrokerRouter(_options);
		}

		[TearDown]
		public void TearDown()
		{
			_router.Dispose();
		}

		[Test]
		[TestCase(1, 1)]
		[TestCase(1, 2)]
		[TestCase(2, 1)]
		[TestCase(2, 2)]
		[TestCase(1, 100)]
		[TestCase(2, 100)]
		[TestCase(50, 10)]
		public async Task ProduceMessages_UpdatesOffsets(int numSets, int numMessagePerSet)
		{
			//Produce a number of messages, and make sure the metadata offsets have updated appropriately.
			//Largely this is checking that we Produce correctly
			
			//Use a new topic for each run. This allows you to debug somewhat by looking at the raw data files in /tmp
			//Clutters things up a bit, though.
			string Topic = string.Format("ProduceMessages_UpdatesOffset_{0}_{1}_{2}", numSets, numMessagePerSet, DateTime.UtcNow.Ticks);
			var mq = new MetadataQueries(_router);

			mq.GetTopic(Topic);
			Thread.Sleep(50); //Wait for server to settle - seems to be necessary for some reason

			_router.RefreshTopicMetadata(Topic);
			var topicMeta = mq.GetTopic(Topic);


			//generate a bunch of messages
			var messageSets = Enumerable.Range(0, numSets).Select(setNum =>
			{
				return new AnnotatedMessageSet()
				{
					Topic = Topic,
					Partition = (setNum % topicMeta.Partitions.Count),
					Messages = Enumerable.Range(0, numMessagePerSet).Select(msgNum =>
					{
						return new Message()
						{
							Value = new byte[] { 255, 254, 253, 252 }
						};
					}).ToList()
				};
			}).ToList();

			//Generate produce request
			var produceRequest = new ProduceRequest()
			{
				Acks = 1,
				TimeoutMS = 10000,
				MessageSets = messageSets
			};

			//send messages
			var route = _router.SelectBrokerRoute(Topic, 0);
			var response = (await route.Connection.SendAsync(produceRequest)).FirstOrDefault();
			Assert.That(response.Error, Is.EqualTo((short)KafkaErrorCode.NoError));

			//Check that messages were added
			//Since we use a new topic each time, we can just add up the offsets
			var newOffsetResponses = await mq.GetTopicOffsetAsync(Topic);
			var msgsAdded = newOffsetResponses.Sum(resp => resp.Offsets.Sum());

			Assert.That(msgsAdded, Is.EqualTo(numSets * numMessagePerSet));
		}

		[Test]
		[TestCase(1, 1)]
		[TestCase(1, 2)]
		[TestCase(2, 1)]
		[TestCase(2, 2)]
		[TestCase(1, 100)]
		[TestCase(2, 100)]
		[TestCase(50, 10)]
		public async Task ProduceGzipMessages_UpdatesOffsets(int numSets, int numMessagePerSet)
		{
			//Produce a number of messages, and make sure the metadata offsets have updated appropriately.
			//Largely this is checking that we Produce correctly

			//Use a new topic for each run. This allows you to debug somewhat by looking at the raw data files in /tmp
			//Clutters things up a bit, though.
			string Topic = string.Format("ProduceMessages_Gzip_UpdatesOffset_{0}_{1}_{2}", numSets, numMessagePerSet, DateTime.UtcNow.Ticks);
			var mq = new MetadataQueries(_router);

			mq.GetTopic(Topic);
			Thread.Sleep(50); //Wait for server to settle - seems to be necessary for some reason

			_router.RefreshTopicMetadata(Topic);
			var topicMeta = mq.GetTopic(Topic);


			//generate a bunch of messages
			var messageSets = Enumerable.Range(0, numSets).Select(setNum =>
			{
				return new AnnotatedMessageSet()
				{
					Topic = Topic,
					Partition = (setNum % topicMeta.Partitions.Count),
					Messages = Enumerable.Range(0, numMessagePerSet).Select(msgNum =>
					{
						return new Message()
						{
							Value = new byte[] { 255, 254, 253, 252 }
						};
					}).ToList()
				};
			}).ToList();

			//Generate produce request
			var produceRequest = new ProduceRequest()
			{
				Acks = 1,
				TimeoutMS = 10000,
				MessageSets = messageSets,
				Codec = MessageCodec.CodecGzip
			};

			//send messages
			var route = _router.SelectBrokerRoute(Topic, 0);
			var response = (await route.Connection.SendAsync(produceRequest)).FirstOrDefault();
			Assert.That(response.Error, Is.EqualTo((short)KafkaErrorCode.NoError));

			//Check that messages were added
			//Since we use a new topic each time, we can just add up the offsets
			var newOffsetResponses = await mq.GetTopicOffsetAsync(Topic);
			var msgsAdded = newOffsetResponses.Sum(resp => resp.Offsets.Sum());

			Assert.That(msgsAdded, Is.EqualTo(numSets * numMessagePerSet));
		}
	}
}

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
	public class FetchTests
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
		public async Task FetchMessages()
		{
			var mq = new MetadataQueries(_router);
			var offsets = await mq.GetTopicOffsetAsync(IntegrationConfig.IntegrationTopic);

			if (!offsets.Any(off => off.Offsets.Max() > 0))
			{
				Assert.Inconclusive("Requires some pre-existing data in the {0} topic", IntegrationConfig.IntegrationTopic);
				return;
			}

			var partition = offsets.First(off => off.Offsets.Max() > 0).PartitionId;
			var route = _router.SelectBrokerRoute(IntegrationConfig.IntegrationTopic, partition);

			var fetchRequest = new FetchRequest()
			{
				Fetches = new List<Fetch>()
				{
					new Fetch(){
						Offset = 0,
						PartitionId = partition,
						Topic = IntegrationConfig.IntegrationTopic
					}
				}
			};

			var response = await route.Connection.SendAsync(fetchRequest);
			Assert.That(response, Is.Not.Null);
			Assert.That(response[0].Error, Is.EqualTo((int)KafkaErrorCode.NoError));
			Assert.That(response[0].Messages.Count, Is.GreaterThan(0));
		}
	}
}

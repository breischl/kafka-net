using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using Common.Logging;

namespace kafka_tests.Integration
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("Integration")]
	[Timeout(10000)]
    public class KafkaConnectionIntegrationTests
    {
        private IKafkaConnection _conn;

        [SetUp]
        public void Setup()
        {
            var options = new KafkaOptions(IntegrationConfig.IntegrationUri);
			_conn = (new DefaultKafkaConnectionFactory()).Create(IntegrationConfig.IntegrationUri, options.ResponseTimeoutMs);
        }

		[TearDown]
		public void TearDown()
		{
			_conn.Dispose();
		}
        
        [Test]
        public async Task EnsureTwoRequestsCanCallOneAfterAnother()
        {
            var result1 = await _conn.SendAsync(new MetadataRequest());
            var result2 = await _conn.SendAsync(new MetadataRequest());
            Assert.That(result1.Count, Is.EqualTo(1));
            Assert.That(result2.Count, Is.EqualTo(1));
        }

        [Test]
        public async Task EnsureAsyncRequestResponsesCorrelate()
        {
            var result1 = _conn.SendAsync(new MetadataRequest());
            var result2 = _conn.SendAsync(new MetadataRequest());
            var result3 = _conn.SendAsync(new MetadataRequest());

            Assert.That((await result1).Count, Is.EqualTo(1));
            Assert.That((await result2).Count, Is.EqualTo(1));
            Assert.That((await result3).Count, Is.EqualTo(1));
        }

        [Test]
        public void EnsureMultipleAsyncRequestsCanReadResponses()
        {
            var requests = new List<Task<List<MetadataResponse>>>();
            var singleResult = _conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationTopic }) }).Result;
            Assert.That(singleResult.Count, Is.GreaterThan(0));
            Assert.That(singleResult.First().Topics.Count, Is.GreaterThan(0));

            for (int i = 0; i < 20; i++)
            {
                requests.Add(_conn.SendAsync(new MetadataRequest()));
            }

            var results = requests.Select(x => x.Result).ToList();
            Assert.That(results.Count, Is.EqualTo(20));
        }

        [Test]
        public async Task EnsureDifferentTypesOfResponsesCanBeReadAsync()
        {
            //just ensure the topic exists for this test
            var ensureTopic = await _conn.SendAsync(new MetadataRequest { Topics = new List<string>(new[] { IntegrationConfig.IntegrationTopic }) });

            Assert.That(ensureTopic.Count, Is.GreaterThan(0));
            Assert.That(ensureTopic.First().Topics.Count, Is.EqualTo(1));
            Assert.That(ensureTopic.First().Topics.First().Name == IntegrationConfig.IntegrationTopic, Is.True, "MetadataRequest did not return expected topic.");


            var result1 = _conn.SendAsync(RequestFactory.CreateProduceRequest(IntegrationConfig.IntegrationTopic, "test"));
            var result2 = _conn.SendAsync(new MetadataRequest());
            var result3 = _conn.SendAsync(RequestFactory.CreateOffsetRequest(IntegrationConfig.IntegrationTopic));
            var result4 = _conn.SendAsync(RequestFactory.CreateFetchRequest(IntegrationConfig.IntegrationTopic, 0));

			await Task.WhenAll(result1, result2, result3, result4);

            Assert.That(result1.Result.Count, Is.EqualTo(1));
            Assert.That(result1.Result.First().Topic == IntegrationConfig.IntegrationTopic, Is.True, "ProduceRequest did not return expected topic.");

            Assert.That(result2.Result.Count, Is.GreaterThan(0));
            Assert.That(result2.Result.First().Topics.Any(x => x.Name == IntegrationConfig.IntegrationTopic), Is.True, "MetadataRequest did not return expected topic.");

            Assert.That(result3.Result.Count, Is.EqualTo(1));
            Assert.That(result3.Result.First().Topic == IntegrationConfig.IntegrationTopic, Is.True, "OffsetRequest did not return expected topic.");

            Assert.That(result4.Result.Count, Is.EqualTo(1));
            Assert.That(result4.Result.First().Topic == IntegrationConfig.IntegrationTopic, Is.True, "FetchRequest did not return expected topic.");

        }
    }
}

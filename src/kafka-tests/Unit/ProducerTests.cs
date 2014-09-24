using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using NUnit.Framework;
using Ninject.MockingKernel.Moq;
using System.Threading;
using kafka_tests.Helpers;


namespace kafka_tests.Unit
{
	[TestFixture]
	[Category("Unit")]
	[Timeout(10000)]
	public class ProducerTests
	{
		private MoqMockingKernel _kernel;
		private BrokerRouterProxy _routerProxy;
		private IBrokerRouter _router;

		[SetUp]
		public void Setup()
		{
			_kernel = new MoqMockingKernel();
			_routerProxy = new BrokerRouterProxy(_kernel);
			_router = _routerProxy.Create();
		}

		[TearDown]
		public void Teardown()
		{
			_router.Dispose();
			_kernel.Dispose();
		}

		[Test]
		public async Task ProducerShouldGroupMessagesByBroker()
		{
			var producer = new Producer(_router);
			var messages = new List<Message>
                {
                    new Message{Value = BitConverter.GetBytes(1)}, new Message{Value = BitConverter.GetBytes(2)}
                };

			var response = await producer.SendMessageAsync("UnitTest", messages);

			Assert.That(response.MessageCount, Is.EqualTo(2));
			Assert.That(_routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
			Assert.That(_routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
		}

		[Test]
		public async Task ProducerShouldGroupMessagesByBrokerAcrossRequests()
		{
			int messageCount = 1000;
			int messagesPerSet = 10;

			var producer = new Producer(new ProducerOptions(_router, maxAccumulationTime: TimeSpan.Zero, maxAccumulationMessages: 10000));

			var tasks = new List<Task<ProduceResult>>(messageCount / messagesPerSet);
			for (int setIdx = 0; setIdx < (messageCount / messagesPerSet); setIdx++)
			{
				var messages = Enumerable.Range(0, messagesPerSet).Select(msgIdx =>
					new Message { Key = BitConverter.GetBytes(msgIdx), Value = (setIdx + "_" + msgIdx).ToUnsizedBytes() }
				).ToList();

				tasks.Add(producer.SendMessageAsync("UnitTest" + setIdx, messages));
			};

			producer.Flush();

			var responses = (await Task.WhenAll(tasks)).ToList();

			Assert.That(responses.Count, Is.EqualTo((messageCount / messagesPerSet)));
			Assert.That(responses.All(r => r != null));
			Assert.That(responses.All(r => r.MessageCount > 5 && r.MessageCount < 20), "Messages should be grouped (those sizes may not be exactly uniform due to uneven hashing)");
			Assert.That(_routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
			Assert.That(_routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
		}

		[Test]
		public async Task ShouldSendAsyncToAllConnectionsEvenWhenExceptionOccursOnOne()
		{
			_routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

			var producer = new Producer(_router);
			var messages = new List<Message>
            {
                new Message{Value = BitConverter.GetBytes(1)}, 
				new Message{Value = BitConverter.GetBytes(2)}
            };

			var produceResult = await producer.SendMessageAsync("UnitTest", messages);
			Assert.That(produceResult.FailedMessages.Count, Is.EqualTo(1));
			Assert.That(produceResult.SuccessfulMessages.Count, Is.EqualTo(1));

			Assert.That(_routerProxy.BrokerConn0.ProduceRequestCallCount, Is.EqualTo(1));
			Assert.That(_routerProxy.BrokerConn1.ProduceRequestCallCount, Is.EqualTo(1));
		}

		[Test]
		[Ignore("is there a way to communicate back which client failed and which succeeded.")]
		public void ConnectionExceptionOnOneShouldCommunicateBackWhichMessagesFailed()
		{
			//TODO is there a way to communicate back which client failed and which succeeded.
			var routerProxy = new BrokerRouterProxy(_kernel);
			routerProxy.BrokerConn1.ProduceResponseFunction = () => { throw new ApplicationException("some exception"); };

			var producer = new Producer(_router);
			var messages = new List<Message>
                {
                    new Message{Value = BitConverter.GetBytes(1)}, new Message{Value = BitConverter.GetBytes(2)}
                };

			//this will produce an exception, but message 1 succeeded and message 2 did not.  
			//should we return a ProduceResponse with an error and no error for the other messages?
			//at this point though the client does not know which message is routed to which server.  
			//the whole batch of messages would need to be returned.
			var test = producer.SendMessageAsync("UnitTest", messages).Result;
		}
	}
}

using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using System.Collections.Generic;

namespace kafka_tests.Integration
{
    /// <summary>
    /// Note these integration tests require an actively running kafka server defined in the app.config file.
    /// </summary>
    [TestFixture]
    [Category("unit")]
	[Timeout(10000)]
    public class KafkaTcpSocketTests
    {
        private readonly Uri _fakeServerUrl;
        private readonly Uri _badServerUrl;

        public KafkaTcpSocketTests()
        {
            _fakeServerUrl = new Uri("http://localhost:8999");
            _badServerUrl = new Uri("http://localhost:1");
        }

        [Test]
        public void KafkaTcpSocketShouldConstruct()
        {
            using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
            {
                Assert.That(test, Is.Not.Null);
                Assert.That(test.ServerUri, Is.EqualTo(_fakeServerUrl));
            }
        }

        #region Dispose Tests...
        [Test]
        public void KafkaTcpSocketShouldDisposeEvenWhilePollingToReconnect()
        {
			using (var server = new FakeTcpServer(8999))
			using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
			{
				var taskResult = test.ReadAsync(4);
				taskResult.Wait(1);

				test.Dispose();

				Assert.Throws<OperationCanceledException>(async () =>
				{
					await taskResult;
				});
			}
        }

		[Test]
		public void KafkaTcpSocketShouldDisposeEvenWhileAwaitingReadAndThrowException()
		{
			using (var server = new FakeTcpServer(8999))
			using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
			{
				var taskResult = test.ReadAsync(4);
				taskResult.Wait(1);

				test.Dispose();

				Assert.Throws<OperationCanceledException>(async () =>
				{
					await taskResult;
				});
			}
		}
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldBlockUntilAllBytesRequestedAreReceived()
        {
			using (var server = new FakeTcpServer(8999))
			{
				var count = 0;

				using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{

					var resultTask = test.ReadAsync(4).ContinueWith(t =>
						{
							Interlocked.Increment(ref count);
							return t.Result;
						});

					Console.WriteLine("Sending first 3 bytes...");
					server.SendDataAsync(new byte[] { 0, 0, 0 }).Wait();

					Console.WriteLine("Ensuring task blocks...");
					var unblocked = resultTask.Wait(TimeSpan.FromMilliseconds(50));
					Assert.That(unblocked, Is.False, "Wait should return false.");
					Assert.That(resultTask.IsCompleted, Is.False, "Task should still be running, blocking.");
					Assert.That(count, Is.EqualTo(0), "Should still block even though bytes have been received.");

					Console.WriteLine("Sending last byte...");
					server.SendDataAsync(new byte[] { 0 }).Wait();

					Console.WriteLine("Ensuring task unblocks...");
					resultTask.Wait(TimeSpan.FromMilliseconds(50));
					Assert.That(resultTask.IsCompleted, Is.True, "Task should have completed.");
					Assert.That(count, Is.EqualTo(1), "Task ContinueWith should have executed.");
					Assert.That(resultTask.Result.Length, Is.EqualTo(4), "Result of task should be 4 bytes.");
				}
			}
        }

        [Test]
        public async Task ReadShouldBeAbleToReceiveMoreThanOnce()
        {
			using (var server = new FakeTcpServer(8999))
			{
				const int firstMessage = 99;
				const string secondMessage = "testmessage";

				using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{
					var readTask = test.ReadAsync(4);
					
					Console.WriteLine("Sending first message to receive...");
					await server.SendDataAsync(firstMessage.ToBytes());

					var firstResponse = (await readTask).ToInt32();
					Assert.That(firstResponse, Is.EqualTo(firstMessage));

					Console.WriteLine("Sending second message to receive...");
					await server.SendDataAsync(secondMessage);

					var secondResponse = Encoding.ASCII.GetString(await test.ReadAsync(secondMessage.Length));
					Assert.That(secondResponse, Is.EqualTo(secondMessage));
				}
			}
        }

        [Test]
        public async Task ReadShouldNotLoseDataFromStreamOverMultipleReads()
        {
            using (var server = new FakeTcpServer(8999))
            {
                const int firstMessage = 99;
                const string secondMessage = "testmessage";

                var payload = new WriteByteStream();
                payload.Pack(firstMessage.ToBytes(), secondMessage.ToBytes());

				using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{
					var readTask = test.ReadAsync(4);

					//send the combined payload
					await server.SendDataAsync(payload.Payload());

					var firstResponse = (await readTask).ToInt32();
					Assert.That(firstResponse, Is.EqualTo(firstMessage));

					var secondResponse = Encoding.ASCII.GetString(await test.ReadAsync(secondMessage.Length));
					Assert.That(secondResponse, Is.EqualTo(secondMessage));
				}
            }
        }

		[Test]
		public void ReadShouldThrowServerDisconnectedExceptionWhenDisconnected()
		{
			using (var server = new FakeTcpServer(8999))
			using (var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
			{
				var resultTask = socket.ReadAsync(4);

				//wait till connected
				TaskTest.WaitFor(() => server.ConnectionEventcount > 0);

				server.DropConnection();

				TaskTest.WaitFor(() => server.DisconnectionEventCount > 0);

				Assert.Throws<ServerDisconnectedException>(async () =>
				{
					await resultTask;
				});
			}
		}

        [Test]
        public async Task ReadShouldReconnectAfterLosingConnection()
        {
            using (var server = new FakeTcpServer(8999))
            {
                var disconnects = 0;
                var connects = 0;
                server.OnClientConnected += () => Interlocked.Increment(ref connects);
                server.OnClientDisconnected += () => Interlocked.Increment(ref disconnects);

				using (var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{
					//wait till connected
					TaskTest.WaitFor(() => connects > 0);

					var readTask = socket.ReadAsync(4);

					//drop connection
					server.DropConnection();
					TaskTest.WaitFor(() => disconnects > 0);
					Assert.That(disconnects, Is.EqualTo(1), "Server should have disconnected the client.");

					Assert.Throws<ServerDisconnectedException>(async () =>
					{
						await readTask;
					});

					//wait for reconnection
					readTask = socket.ReadAsync(4);
					TaskTest.WaitFor(() => connects > 1);
					Assert.That(connects, Is.EqualTo(2), "Socket should have reconnected.");

					//send data and get result
					await server.SendDataAsync(99.ToBytes());
					var result = (await readTask).ToInt32();
					Assert.That(result, Is.EqualTo(99), "Socket should have received the 4 bytes.");
				}
            }
        }

        [Test]
        public async Task ReadShouldStackReadRequestsAndReturnOneAtATime()
        {
            using (var server = new FakeTcpServer(8999))
			using (var socket = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
			{
                var messages = new[]{"test1", "test2", "test3", "test4"};
                var expectedLength = "test1".Length;

                var payload = new WriteByteStream();
                payload.Pack(messages.Select(x => x.ToBytes()).ToArray());

                var tasks = messages.Select(x => socket.ReadAsync(x.Length)).ToArray();

                await server.SendDataAsync(payload.Payload());

                foreach (var task in tasks)
                {
                    Assert.That((await task).Length, Is.EqualTo(expectedLength));
                }
            }
        }
        #endregion

        #region Write Tests...
        [Test]
        public async Task WriteAsyncShouldSendData()
        {
            using (var server = new FakeTcpServer(8999))
            {
                const int testData = 99;
                int result = 0;

				using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{
					server.OnBytesReceived += data => result = data.ToInt32();

					await test.WriteAsync(testData.ToBytes());
					TaskTest.WaitFor(() => result > 0);
					Assert.That(result, Is.EqualTo(testData));
				}
            }
        }

        [Test]
        public void WriteAsyncShouldAllowMoreThanOneWrite()
        {
            using (var server = new FakeTcpServer(8999))
            {
                const int testData = 99;
                var results = new List<byte>();

				using (var test = new KafkaTcpSocket(new DefaultTraceLog(), _fakeServerUrl))
				{
					server.OnBytesReceived += results.AddRange;

					Task.WaitAll(test.WriteAsync(testData.ToBytes()),
								test.WriteAsync(testData.ToBytes()));

					TaskTest.WaitFor(() => results.Count >= 8);
					Assert.That(results.Count, Is.EqualTo(8));
				}
            }
        }
        #endregion
    }
}

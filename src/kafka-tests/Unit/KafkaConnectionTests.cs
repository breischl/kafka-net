using System;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Common;
using KafkaNet.Protocol;
using Moq;
using NUnit.Framework;
using Ninject.MockingKernel.Moq;
using kafka_tests.Helpers;

namespace kafka_tests.Unit
{
    [TestFixture]
    public class KafkaConnectionTests
    {
        private MoqMockingKernel _kernel;

        [SetUp]
        public void Setup()
        {
            _kernel = new MoqMockingKernel();
        }

        #region Dispose Tests...
        [Test]
        public void ShouldDisposeWithoutExceptionThrown()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(new Uri("http://localhost:8999")))
            {
                var conn = new KafkaConnection(socket);
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                using (conn) { }
            }
        }

        [Test]
        public void ShouldDisposeWithoutExceptionEvenWhileCallingSendAsync()
        {
            using (var socket = new KafkaTcpSocket(new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket))
            {
                var task = conn.SendAsync(new MetadataRequest());
                task.Wait(TimeSpan.FromMilliseconds(1000));
                Assert.That(task.IsCompleted, Is.False, "The send task should still be pending.");
            }
        }
        #endregion

        #region Read Tests...
        [Test]
        public void ReadShouldIgnoreMessageWithUnknownCorrelationId()
        {
            const int correlationId = 99;

            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket))
            {
                //send correlation message
                server.SendDataAsync(CreateCorrelationMessage(correlationId)).Wait(TimeSpan.FromSeconds(1));

                //wait for connection
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));
            }
        }
        #endregion

        #region Send Tests...
        [Test]
        public void SendAsyncShouldTimeoutByThrowingResponseTimeoutException()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket, 100))
            {
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var taskResult = conn.SendAsync(new MetadataRequest());

                taskResult.ContinueWith(t => taskResult = t).Wait(TimeSpan.FromSeconds(1));

                Assert.That(taskResult.IsFaulted, Is.True);
                Assert.That(taskResult.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>());
            }
        }

        [Test]
        public void SendAsyncShouldTimeoutMultipleMessagesAtATime()
        {
            using (var server = new FakeTcpServer(8999))
            using (var socket = new KafkaTcpSocket(new Uri("http://localhost:8999")))
            using (var conn = new KafkaConnection(socket, 100))
            {
                TaskTest.WaitFor(() => server.ConnectionEventcount > 0);
                Assert.That(server.ConnectionEventcount, Is.EqualTo(1));

                var tasks = new[]
                    {
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest()),
                        conn.SendAsync(new MetadataRequest())
                    };

                Task.WhenAll(tasks);

                TaskTest.WaitFor(() => tasks.Any(t => t.IsFaulted ));
                foreach (var task in tasks)
                {
                    Assert.That(task.IsFaulted, Is.True);
                    Assert.That(task.Exception.InnerException, Is.TypeOf<ResponseTimeoutException>());
                }
            }
        }
        #endregion

        private static byte[] CreateCorrelationMessage(int id)
        {
            var stream = new WriteByteStream();
            stream.Pack(4.ToBytes(), id.ToBytes());
            return stream.Payload();
        }
    }
}

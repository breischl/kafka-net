using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Protocol;
using Common.Logging;

namespace KafkaNet
{
    /// <summary>
    /// KafkaConnection represents the lowest level TCP stream connection to a Kafka broker. 
    /// The Send and Receive are separated into two disconnected paths and must be combine outside
    /// this class by the correlation ID contained within the returned message.
    /// 
    /// The SendAsync function will return a Task and complete once the data has been sent to the outbound stream.
    /// The Read response is handled by a single thread polling the stream for data and firing an OnResponseReceived
    /// event when a response is received.
    /// </summary>
    public class KafkaConnection : IKafkaConnection
    {
        private const int DefaultResponseTimeoutMs = 30000;
		private static int NextConnectionId = 0;

        private readonly ConcurrentDictionary<int, AsyncRequestItem> _requestIndex = new ConcurrentDictionary<int, AsyncRequestItem>();
        private readonly IScheduledTimer _responseTimeoutTimer;
        private readonly int _responseTimeoutMS;
		private readonly ILog _log = LogManager.GetLogger<KafkaConnection>();
        private readonly IKafkaTcpSocket _client;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
		private readonly BlockingCollection<byte[]> _sendQueue = new BlockingCollection<byte[]>(new ConcurrentQueue<byte[]>());
        private int _correlationIdSeed;
		public readonly int _connectionId;

        /// <summary>
        /// Initializes a new instance of the KafkaConnection class.
        /// </summary>
        /// <param name="log">Logging interface used to record any log messages created by the connection.</param>
        /// <param name="client">The kafka socket initialized to the kafka server.</param>
        /// <param name="responseTimeoutMs">The amount of time to wait for a message response to be received after sending message to Kafka.</param>
        public KafkaConnection(IKafkaTcpSocket client, int responseTimeoutMs = DefaultResponseTimeoutMs)
        {
			_client = client;
            _responseTimeoutMS = responseTimeoutMs;
            _responseTimeoutTimer = new ScheduledTimer()
                .Do(ResponseTimeoutCheck)
                .Every(TimeSpan.FromMilliseconds(100))
                .StartingAt(DateTime.Now.AddMilliseconds(_responseTimeoutMS))
                .Begin();
			_connectionId = Interlocked.Increment(ref NextConnectionId);


			StartReceiveProc();
			StartSendProc();
        }

		public bool IsOpen
		{
			get
			{
				return !_disposeToken.IsCancellationRequested;
			}
		}

		public int ConnectionId
		{
			get
			{
				return _connectionId;
			}
		}

        /// <summary>
        /// Uri connection to kafka server.
        /// </summary>
        public Uri KafkaUri
        {
            get { return _client.ServerUri; }
        }
		
        /// <summary>
        /// Send kafka payload to server and receive a task event when response is received.
        /// </summary>
        /// <typeparam name="T">A Kafka response object return by decode function.</typeparam>
        /// <param name="request">The IKafkaRequest to send to the kafka servers.</param>
        /// <returns></returns>
        public async Task<List<T>> SendAsync<T>(IKafkaRequest<T> request)
        {
			if (_disposeToken.Token.IsCancellationRequested)
			{
				throw new ObjectDisposedException("KafkaConnection");
			}

            //assign unique correlationId
            request.CorrelationId = NextCorrelationId();

            var asyncRequest = new AsyncRequestItem(request.CorrelationId);

			if (_requestIndex.TryAdd(request.CorrelationId, asyncRequest) == false)
			{
				throw new ApplicationException("Failed to register request for async response.");
			}

			var encodedRequest = request.Encode();
			_sendQueue.Add(encodedRequest);

            var response = await asyncRequest.ReceiveTask.Task;

            return request.Decode(response).ToList();
        }
		
		private void StartSendProc()
		{
			Task.Run(async delegate
			{
				try
				{
					var enumerator = _sendQueue.GetConsumingEnumerable(_disposeToken.Token);
					foreach (var payload in enumerator)
					{
						bool sent = false;

						while (!sent)
						{
							try
							{
								await _client.WriteAsync(payload);
								sent = true;
							}
							catch (ServerDisconnectedException ex)
							{
								if (_disposeToken.Token.IsCancellationRequested)
								{
									_log.DebugFormat("KafkaConnection cancellation requested, exiting");
									return;
								}
								else
								{
									_log.WarnFormat("Caught unexpected ServerDisconnectedException in connection {0} to server {1}", ex, _connectionId, _client.ServerUri);
								}
							}
							catch (Exception ex)
							{
								_log.ErrorFormat("Error sending to server {0} in connection {1}", ex, _client.ServerUri, _connectionId);
								Thread.Sleep(100);
							}
						}
					}
				}
				finally
				{
					_log.TraceFormat("Exiting send loop for connection {0} to server {1}", _connectionId, _client.ServerUri);
					_disposeToken.Cancel(); //if the loop crashed, close the whole connection
				}
			});
		}

		private void StartReceiveProc()
		{
			Task.Run(async delegate
			{
				try
				{
					//This thread will poll the receive stream for data, parse a message out
					//and trigger an event with the message payload
					while (_disposeToken.Token.IsCancellationRequested == false)
					{
						try
						{
							_log.TraceFormat("Connection {0} awaiting message from {1}", _connectionId, KafkaUri);
							var messageSize = (await _client.ReadAsync(4)).ToInt32();

							_log.TraceFormat("Connection {0} received message of size {1} from {2}", _connectionId, messageSize, KafkaUri);
							var message = await _client.ReadAsync(messageSize);

							CorrelatePayloadToRequest(message);
						}
						catch (ServerDisconnectedException ex)
						{
							_log.DebugFormat("Connection {0} to server {1} canceled", _connectionId, _client.ServerUri);
							SetOutstandingRequestFault(ex);
						}
						catch (ObjectDisposedException ode)
						{
							throw new OperationCanceledException("Object is being disposed", ode);
						}
						catch (Exception ex)
						{
							_log.ErrorFormat("Exception occured in polling read thread in connection {0} for server {1}", ex, _connectionId, _client.ServerUri);
							SetOutstandingRequestFault(ex);
						}
					}
				}
				finally
				{
					_log.TraceFormat("Exiting receive loop for connection {0} to server {1}", _connectionId, _client.ServerUri);
					_disposeToken.Cancel(); //if the loop crashed, close the connection
				}
			});
		}

        private void CorrelatePayloadToRequest(byte[] payload)
        {
            var correlationId = payload.Take(4).ToArray().ToInt32();
            AsyncRequestItem asyncRequest;
            if (_requestIndex.TryRemove(correlationId, out asyncRequest))
            {
                asyncRequest.ReceiveTask.SetResult(payload);
            }
            else
            {
                _log.WarnFormat("Message response received with correlationId={0}, but did not exist in the request queue.", correlationId);
            }
        }

        private int NextCorrelationId()
        {
            var id = Interlocked.Increment(ref _correlationIdSeed);
            if (id > int.MaxValue - 1000) //somewhere close to max reset.
            {
				Interlocked.Exchange(ref _correlationIdSeed, 0);
            }
            return id;
        }

        /// <summary>
        /// Iterates the waiting response index for any requests that should be timed out and marks as exception.
        /// </summary>
		private void ResponseTimeoutCheck()
		{
			lock (_responseTimeoutTimer)
			{
				if (_disposeToken.Token.IsCancellationRequested)
				{
					foreach (var request in _requestIndex.Values)
					{
						request.ReceiveTask.SetCanceled();
					}
					_requestIndex.Clear();
				}
				else
				{
					var timeouts = _requestIndex.Values.Where(x => x.CreatedOnUtc.AddMilliseconds(_responseTimeoutMS) < DateTime.UtcNow).ToList();

					foreach (var timeout in timeouts)
					{
						AsyncRequestItem request;
						if (_requestIndex.TryRemove(timeout.CorrelationId, out request))
						{
							request.ReceiveTask.TrySetException(new ResponseTimeoutException("Timeout Expired. Client failed to receive a response from server after waiting " + DefaultResponseTimeoutMs + "ms."));
						}
					}
				}
			}
		}

		#region Equals Override...
		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((KafkaConnection)obj);
		}

		protected bool Equals(KafkaConnection other)
		{
			return Equals(KafkaUri, other.KafkaUri);
		}

		public override int GetHashCode()
		{
			return (KafkaUri != null ? KafkaUri.GetHashCode() : 0);
		}
		#endregion

		private void SetOutstandingRequestFault(Exception ex)
		{
			var failedRequests = _requestIndex.Values;
			_requestIndex.Clear();

			foreach (var request in failedRequests)
			{
				request.ReceiveTask.SetException(ex);
			}
		}

		public void Dispose()
		{
			_log.DebugFormat("Disposing connection {0} to server {1}", _connectionId, _client.ServerUri);
			_client.Dispose();
			_responseTimeoutTimer.Dispose();
			_disposeToken.Cancel();
			ResponseTimeoutCheck();
		}

        class AsyncRequestItem
        {
            public AsyncRequestItem(int correlationId)
            {
                CorrelationId = correlationId;
                CreatedOnUtc = DateTime.UtcNow;
                ReceiveTask = new TaskCompletionSource<byte[]>();
            }

            public int CorrelationId { get; private set; }
            public TaskCompletionSource<byte[]> ReceiveTask { get; private set; }
            public DateTime CreatedOnUtc { get; private set; }
        }
    }


}

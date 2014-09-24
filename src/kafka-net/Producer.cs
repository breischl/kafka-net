using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Threading;
using KafkaNet.Common;
using Common.Logging;


namespace KafkaNet
{
    /// <summary>
    /// Provides a simplified high level API for sending messages to Kafka
    /// </summary>
	public class Producer : IMetadataQueries
	{
		private static readonly ILog _log = LogManager.GetLogger<Producer>();
		private readonly ProducerOptions _opts;
		private Queue<QueuedSendRequest> _pendingMessages;
		private IScheduledTimer _sendTimer;
		private int _enqueuedMessageCount = 0;

		/// <summary>
		/// Construct a Producer class.
		/// </summary>
		/// <param name="brokerRouter">The router used to direct produced messages to the correct partition.</param>
		public Producer(IBrokerRouter brokerRouter)
			: this(new ProducerOptions(brokerRouter))
		{
		}

		public Producer(ProducerOptions opts)
		{
			_opts = opts;

			var initialQueueSize = (int)((float)(opts.MaxAccumulationMessages ?? 500) * 1.1);
			_pendingMessages = new Queue<QueuedSendRequest>(initialQueueSize);

			if (_opts.MaxAccumulationTime > TimeSpan.Zero)
			{
				_sendTimer = new ScheduledTimer()
								.Every(_opts.SendTimeout)
								.Do(Flush);
			}
			else
			{
				_sendTimer = null;
			}
		}

		/// <summary>
		/// Send a enumerable of message objects to a given topic.
		/// </summary>
		/// <param name="topic">The name of the kafka topic to send the messages to.</param>
		/// <param name="messages">The enumerable of messages that will be sent to the given topic.</param>
		public Task<ProduceResult> SendMessageAsync(string topic, IEnumerable<Message> messages)
		{
			var sendReq = new QueuedSendRequest(topic, messages);

			lock (_pendingMessages)
			{
				_pendingMessages.Enqueue(sendReq);

				var oldMsgCount = _enqueuedMessageCount;
				var newMsgCount = oldMsgCount + sendReq.Messages.Count;

				if (_opts.MaxAccumulationMessages.HasValue && newMsgCount > _opts.MaxAccumulationMessages)
				{
					Task.Run((Action)Flush);
				}
				else if (oldMsgCount == 0 && _sendTimer != null && !_sendTimer.Enabled)
				{
					_sendTimer.Begin();
				}

				_enqueuedMessageCount = newMsgCount;
			}

			return sendReq.Completion.Task;
		}

		/// <summary>
		/// Immediately flush any queued messages to the Kafka server(s)
		/// </summary>
		/// <returns></returns>
		public void Flush()
		{
			List<QueuedSendRequest> sendRequests;
			lock (_pendingMessages)
			{
				if (!_pendingMessages.Any())
				{
					//The timer and the queued message count happened to fire at the same time
					return;
				}

				sendRequests = new List<QueuedSendRequest>(_pendingMessages);
			
				_pendingMessages.Clear();
				_enqueuedMessageCount = 0;
				if (_sendTimer != null)
				{
					_sendTimer.End(); //stop the timer until another message comes in
				}
			}

			//Now we have a complicated mess
			//Each of the original send requests can have many message, each of which goes to the leader of the topic-partition
			//So different requests could each send any number of messages to any of the servers. 
			//We want to send just one to each server, but also track which requests completed in case senders are waiting
			//So we're going to build up a list of messages for each server, and a TaskCompletionSource for the request to that server.
			var serverMessageMap = new Dictionary<Uri, List<RoutedMessages>>();
			
			foreach (var sendRequest in sendRequests)
			{
				var routedMessageGroups = sendRequest.Messages
											.GroupBy(msg => _opts.Router.SelectBrokerRoute(sendRequest.Topic, msg.Key))
											.ToList();
				var routedMessages = routedMessageGroups.Select(grp => new RoutedMessages{ Route = grp.Key, SendRequest = sendRequest, Messages = grp.ToList() });
				var serverGroups = routedMessages.GroupBy(rg => rg.Route.Connection.Endpoint.ServerUri);

				foreach (var serverGroup in serverGroups)
				{
					var routedMessagesForServer = serverGroup.ToList();

					List<RoutedMessages> msgList;
					if (!serverMessageMap.TryGetValue(serverGroup.Key, out msgList))
					{
						serverMessageMap[serverGroup.Key] = routedMessagesForServer;
					}
					else
					{
						msgList.AddRange(routedMessagesForServer);
					}
				}
			}

			//Send the requests
			var continuationTasks = new List<Task>(serverMessageMap.Count);
			foreach (var kvp in serverMessageMap)
			{
				var routedMessages = kvp.Value;

				var payload = (from rm in routedMessages
							   select new AnnotatedMessageSet{
								   Topic = rm.Route.Topic,
								   Partition = rm.Route.PartitionId,
								   Messages = rm.Messages
							   }).ToList();

				var request = new ProduceRequest
				{
					Acks = _opts.Acks,
					TimeoutMS = (int)_opts.SendTimeout.TotalMilliseconds,
					MessageSets = payload,
					Codec = _opts.MessageCodec
				};

				var connection = kvp.Value.First().Route.Connection; //doesn't really matter which route we choose, we already made them all the same server
				var sendTask = connection.SendAsync(request);
				
				var continuationTask = sendTask.ContinueWith(ProcessSendTaskCompletion, routedMessages);
				continuationTasks.Add(continuationTask);
			}

			Task.WaitAll(continuationTasks.ToArray());
			foreach (var sendRequest in sendRequests)
			{
				sendRequest.Complete();
			}
		}

		private void ProcessSendTaskCompletion(Task<List<ProduceResponse>> antecedent, object stateObj)
		{
			var routedMessages = (List<RoutedMessages>)stateObj;

			if (antecedent.IsCanceled)
			{
				var ex = new TaskCanceledException();
				ProcessRoutedMessages(routedMessages, (produceResult, msg) => produceResult.FailedMessages[msg] = ex);
			}
			else if (antecedent.IsFaulted)
			{
				Exception ex = antecedent.Exception;
				if (antecedent.Exception.InnerExceptions.Count == 1)
				{
					ex = ex.InnerException;
				}

				ProcessRoutedMessages(routedMessages, (produceResult, msg) => produceResult.FailedMessages[msg] = ex);
			}
			else if (antecedent.IsCompleted)
			{
				ProcessRoutedMessages(routedMessages, (produceResult, msg) => produceResult.SuccessfulMessages.Add(msg));
			}
		}

		/// <summary>
		/// Helper function to add results to the appropriate ProduceResults for a set of RoutedMessages
		/// </summary>
		private void ProcessRoutedMessages(ICollection<RoutedMessages> routedMessages, Action<ProduceResult, Message> action)
		{
			foreach (var routedMessage in routedMessages)
			{
				var sendRequest = routedMessage.SendRequest;
				foreach (var msg in routedMessage.Messages)
				{
					if (sendRequest.Messages.Contains(msg))
					{
						if (sendRequest.Completion.Task.IsCompleted)
						{
							throw new ApplicationException("Sendrequest was improperly marked as completed");
						}

						action(sendRequest.Result, msg);
					}
				}
			}
		}

		public Topic GetTopic(string topic)
		{
			return (new MetadataQueries(_opts.Router)).GetTopic(topic);
		}

		public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
		{
			return new MetadataQueries(_opts.Router).GetTopicOffsetAsync(topic, maxOffsets, time);
		}

		class QueuedSendRequest
		{
			public readonly string Topic;
			public readonly HashSet<Message> Messages;
			public readonly TaskCompletionSource<ProduceResult> Completion;
			public readonly ProduceResult Result;

			public QueuedSendRequest(string topic, IEnumerable<Message> messages)
			{
				this.Topic = topic;
				this.Messages = new HashSet<Message>(messages);
				this.Completion = new TaskCompletionSource<ProduceResult>();
				this.Result = new ProduceResult(this.Messages.Count);
			}

			public bool HasAllExpectedResults
			{
				get
				{
					return ((this.Result.SuccessfulMessages.Count + this.Result.FailedMessages.Count) >= this.Messages.Count);
				}
			}

			public void Complete()
			{
				this.Completion.SetResult(this.Result);
			}
		}

		class RoutedMessages
		{
			public BrokerRoute Route;
			public IList<Message> Messages;
			public QueuedSendRequest SendRequest;
		}
	}

}

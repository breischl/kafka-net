using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Common.Logging;

namespace KafkaNet
{
    /// <summary>
    /// Provides a basic consumer of one Topic across all partitions or over a given whitelist of partitions.
    /// 
    /// TODO: provide automatic offset saving when the feature is available in 0.8.2
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class Consumer : IMetadataQueries, IDisposable
    {
		private static readonly ILog _log = LogManager.GetLogger<Consumer>();
        private readonly ConsumerOptions _options;
        private readonly BlockingCollection<Message> _fetchResponseQueue;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly ConcurrentDictionary<int, Task> _partitionPollingIndex = new ConcurrentDictionary<int, Task>();
        private readonly ConcurrentDictionary<int, long> _partitionOffsetIndex = new ConcurrentDictionary<int, long>();
        private readonly IScheduledTimer _topicPartitionQueryTimer;
        private readonly IMetadataQueries _metadataQueries;

        private int _ensureOneThread;
        private Topic _topic;

        public Consumer(ConsumerOptions options, params OffsetPosition[] positions)
        {
            _options = options;
            _fetchResponseQueue = new BlockingCollection<Message>(_options.ConsumerBufferSize);
            _metadataQueries = new MetadataQueries(_options.Router);

            //this timer will periodically look for new partitions and automatically add them to the consuming queue
            //using the same whitelist logic
            _topicPartitionQueryTimer = new ScheduledTimer()
                .Do(RefreshTopicPartitions)
                .Every(TimeSpan.FromMilliseconds(_options.TopicPartitionQueryTimeMs))
                .StartingAt(DateTime.Now);

			foreach (var position in positions)
			{
				var temp = position;
				_partitionOffsetIndex.AddOrUpdate(position.PartitionId, _ => temp.Offset, (_, __) => temp.Offset);
			}
        }

        /// <summary>
        /// Get the number of tasks created for consuming each partition.
        /// </summary>
        public int ConsumerTaskCount { get { return _partitionPollingIndex.Count; } }

        /// <summary>
        /// Returns a blocking enumerable of messages received from Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public IEnumerable<Message> Consume(CancellationToken? cancellationToken = null)
        {
            _log.DebugFormat("Beginning consumption of topic: {0}", _options.Topic);
            _topicPartitionQueryTimer.Begin();

			var cancelToken = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken ?? CancellationToken.None).Token;
            return _fetchResponseQueue.GetConsumingEnumerable(cancelToken);
        }
		
        /// <summary>
        /// Get the current running position (offset) for all consuming partition.
        /// </summary>
        /// <returns>List of positions for each consumed partitions.</returns>
        /// <remarks>Will only return data if the consumer is actively being consumed.</remarks>
        public List<OffsetPosition> GetOffsetPosition()
        {
            return _partitionOffsetIndex.Select(x => new OffsetPosition { PartitionId = x.Key, Offset = x.Value }).ToList();
        }

        private void RefreshTopicPartitions() 
        {
            try
            {
                if (Interlocked.Increment(ref _ensureOneThread) == 1)
                {
                    _log.DebugFormat("Refreshing partitions for topic: {0}", _options.Topic);
                    var topic = _options.Router.GetTopicMetadata(_options.Topic);
                    if (topic.Count <= 0) throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", _options.Topic));
                    _topic = topic.First();

                    //create one Task per partitions, if they are in the white list.
					IEnumerable<Partition> pollingPartitions = _topic.Partitions;
					if (_options.PartitionWhitelist.Any())
					{
						pollingPartitions = pollingPartitions.Where(p => _options.PartitionWhitelist.Contains(p.PartitionId));
					}

					foreach (var partition in pollingPartitions)
					{
						_partitionPollingIndex.AddOrUpdate(partition.PartitionId,
															i => ConsumeTopicPartitionAsync(_topic.Name, partition.PartitionId),
															(i, task) => task);
					}
                }
            }
            catch (Exception ex)
            {
                _log.ErrorFormat("Exception occured trying to setup consumer for topic:{0}.  Exception={1}", _options.Topic, ex);
            }
            finally
            {
                Interlocked.Decrement(ref _ensureOneThread);
            }
        }

        private Task ConsumeTopicPartitionAsync(string topic, int partitionId)
        {
            return Task.Run(async delegate
            {
				try
				{
					_log.DebugFormat("Creating polling task for topic: {0} parition: {1}", topic, partitionId);
					while (_disposeToken.IsCancellationRequested == false)
					{
						try
						{
							//get the current offset, or default to zero if not there.
							long offset = _partitionOffsetIndex.AddOrUpdate(partitionId, i => 0, (i, currentOffset) => currentOffset);

							//build a fetch request for partition at offset
							var fetches = new List<Fetch>{
												new Fetch
													{
														Topic = topic,
														PartitionId = partitionId,
														Offset = offset
													}
											};

							var fetchRequest = new FetchRequest
							{
								Fetches = fetches,
								
								//for some reason setting these cause tests to intermittently time out.
								//MinBytes = 100,
								//MaxWaitTime = 10000
							};

							//make request and post to queue
							var route = _options.Router.SelectBrokerRoute(topic, partitionId);
							var responses = await route.Connection.SendAsync(fetchRequest);
							_disposeToken.Token.ThrowIfCancellationRequested();

							if (responses.Count > 0)
							{
								var response = responses.FirstOrDefault(); //we only asked for one response
								if (response != null && response.Messages.Count > 0)
								{
									foreach (var message in response.Messages)
									{
										_fetchResponseQueue.Add(message, _disposeToken.Token);
									}

									var nextOffset = response.Messages.Max(x => x.Meta.Offset) + 1;
									_partitionOffsetIndex.AddOrUpdate(partitionId, i => nextOffset, (i, l) => nextOffset);
									continue;
								}
							}

							//no message received from server wait a while before we try another long poll
							await Task.Delay(_options.BackoffInterval, _disposeToken.Token);
						}
						catch (OperationCanceledException)
						{
							_log.DebugFormat("Consumer operation cancelled");
							return;
						}
						catch (ResponseTimeoutException)
						{
							//long-poll finished, just start it again
							continue;
						}
						catch (Exception ex)
						{
							_log.ErrorFormat("Exception occured while polling topic:{0} partition:{1}.  Polling will continue.  Exception={2}", topic, partitionId, ex);
						}
					}
				}
				finally
				{
					_log.DebugFormat("Disabling polling task for topic: {0} on parition: {1}", topic, partitionId);
					Task tempTask;
					_partitionPollingIndex.TryRemove(partitionId, out tempTask);
				}
            });
        }

        public Topic GetTopic(string topic)
        {
            return _metadataQueries.GetTopic(topic);
        }

        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        public void Dispose()
        {
            _log.DebugFormat("Disposing...");
            _disposeToken.Cancel();
			_topicPartitionQueryTimer.Dispose();
        }
    }
}

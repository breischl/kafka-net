using System.Collections.Generic;
using System.Text;
using KafkaNet.Protocol;

namespace kafka_tests
{
    public static class RequestFactory
    {
		internal static ProduceRequest CreateProduceRequest(string topic, string message)
        {
            return new ProduceRequest
                {
                    MessageSets = new List<AnnotatedMessageSet>(new[]
                        {
                            new AnnotatedMessageSet
                                {
                                    Topic = topic,
                                    Messages = new List<Message>(new[] {new Message {Value = Encoding.UTF8.GetBytes(message)}})
                                }
                        })
                };
        }

        public static FetchRequest CreateFetchRequest(string topic, int offset, int partitionId = 0)
        {
            return new FetchRequest
            {
                CorrelationId = 1,
                Fetches = new List<FetchRequestItem>(new[]
                        {
                            new FetchRequestItem
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    Offset = offset
                                }
                        })
            };
        }

        public static OffsetRequest CreateOffsetRequest(string topic, int partitionId = 0, int maxOffsets = 1, int time = -1)
        {
            return new OffsetRequest
            {
                CorrelationId = 1,
                Offsets = new List<Offset>(new[]
                        {
                            new Offset
                                {
                                    Topic = topic,
                                    PartitionId = partitionId,
                                    MaxOffsets = maxOffsets,
                                    Time = time
                                }
                        })
            };
        }

        public static OffsetFetchRequest CreateOffsetFetchRequest(string topic, int partitionId = 0)
        {
            return new OffsetFetchRequest
            {
                ConsumerGroup = "DefaultGroup",
                Topics = new List<OffsetFetch>(new[] 
        		                          {
        		                          	new OffsetFetch
        		                          	{
        		                          		Topic = topic,
        		                          		PartitionId = partitionId
        		                          	}
        		                          })
            };
        }
    }
}

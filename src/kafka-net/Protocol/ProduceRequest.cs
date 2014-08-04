using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    internal class ProduceRequest : BaseRequest, IKafkaRequest<ProduceResponse>
    {
        public ProduceRequest()
        {
        }

        /// <summary>
        /// Indicates the type of kafka encoding this request is.
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Produce; } }

        /// <summary>
        /// Time kafka will wait for requested ack level before returning.
        /// </summary>
        public int TimeoutMS = 1000;
        
		/// <summary>
        /// Level of ack required by kafka.  0 immediate, 1 written to leader, 2+ replicas synced, -1 all replicas
        /// </summary>
        public Int16 Acks = 1;
       
		/// <summary>
        /// Collection of payloads to post to kafka
        /// </summary>
		internal ICollection<AnnotatedMessageSet> MessageSets;

		public MessageCodec Codec { get; set; }

		public byte[] Encode()
		{
			return EncodeProduceRequest(this);
		}

        public IEnumerable<ProduceResponse> Decode(byte[] payload)
        {
            return DecodeProduceResponse(payload);
        }

		private byte[] EncodeProduceRequest(ProduceRequest request)
		{
			/**
			 * ProduceRequest format (from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ProduceAPI)
			 ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
							  RequiredAcks => int16
							  Timeout => int32
							  Partition => int32
							  MessageSetSize => int32
			 */

			//Because of the need to prepend arrays with length in bytes, we're going to build it up more-or-less backwards

			if (request.MessageSets == null)
			{
				//TODO: Why should we even send a produce request with no messages??
				request.MessageSets = new List<AnnotatedMessageSet>(0);
			}

			var topicPartitionSets = (from msg in request.MessageSets
									  group msg by new
									  {
										  msg.Topic,
										  msg.Partition
									  }
									into topicPartitionGroup
									select new TopicPartitionSet()
									{
										Topic = topicPartitionGroup.Key.Topic,
										Partition = topicPartitionGroup.Key.Partition,
										MessageSets = topicPartitionGroup.ToList()
									})
									.ToList();

			var topicSets = (from tpSet in topicPartitionSets
							 group tpSet by tpSet.Topic into topicGroup
							 select new TopicSet()
							 {
								 Topic = topicGroup.Key,
								 PartitionSets = topicGroup.ToList()
							 })
							 .ToList();

			var encodedTopics = new WriteByteStream();
			encodedTopics.Pack(topicSets.Count.ToBytes());
			foreach (var topicSet in topicSets)
			{
				encodedTopics.Pack(topicSet.Topic.ToInt16SizedBytes());
				encodedTopics.Pack(topicSet.PartitionSets.Count.ToBytes());

				foreach (var tpSet in topicSet.PartitionSets)
				{
					MessageSet encodingSet;
					if (tpSet.MessageSets.Count == 1)
					{
						encodingSet = tpSet.MessageSets.First();
					}
					else
					{
						encodingSet = new MessageSet()
						{
							Messages = tpSet.MessageSets.SelectMany(msgSet => msgSet.Messages).ToList()
						};
					}

					var tpSetBytes = encodingSet.Encode(request.Codec);

					encodedTopics.Pack(tpSet.Partition.ToBytes());
					encodedTopics.Pack(tpSetBytes.Length.ToBytes());
					encodedTopics.Pack(tpSetBytes);
				}
			}

			var byteStream = new WriteByteStream();
			byteStream.Pack(EncodeHeader(request)); //header
			byteStream.Pack(request.Acks.ToBytes(), request.TimeoutMS.ToBytes()); //metadata
			byteStream.Pack(encodedTopics.Payload()); //topics array

			//prepend final messages size
			byteStream.Prepend(byteStream.Length().ToBytes());

			return byteStream.Payload();
		}

        private IEnumerable<ProduceResponse> DecodeProduceResponse(byte[] data)
        {
            var stream = new ReadByteStream(data);

            var correlationId = stream.ReadInt();

            var topicCount = stream.ReadInt();
            for (int i = 0; i < topicCount; i++)
            {
                var topic = stream.ReadInt16String();

                var partitionCount = stream.ReadInt();
                for (int j = 0; j < partitionCount; j++)
                {
                    var response = new ProduceResponse()
                    {
                        Topic = topic,
                        PartitionId = stream.ReadInt(),
                        Error = stream.ReadInt16(),
                        Offset = stream.ReadLong()
                    };

                    yield return response;
                }
            }
        }
        
		/// <summary>
		/// Helper class for encoding
		/// </summary>
		private sealed class TopicSet
		{
			public string Topic;
			public List<TopicPartitionSet> PartitionSets;
		}

		/// <summary>
		/// Helper class for encoding
		/// </summary>
		private class TopicPartitionSet
		{
			public string Topic;
			public int Partition;
			public List<AnnotatedMessageSet> MessageSets;
		}
    }

    
}
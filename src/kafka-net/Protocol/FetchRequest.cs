using System;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
    public class FetchRequest : BaseRequest, IKafkaRequest<FetchResponse>
    {
        internal const int DefaultMinBlockingByteBufferSize = 4096;
        private const int DefaultMaxBlockingWaitTime = 500;

        /// <summary>
        /// Indicates the type of kafka encoding this request is
        /// </summary>
        public ApiKeyRequestType ApiKey { get { return ApiKeyRequestType.Fetch; } }
        
		/// <summary>
        /// The maximum amount of time to block for the MinBytes to be available before returning.
		/// Note that setting this too long may result in client timeouts happening before the server returns. 
		/// Recommended maximum setting is 5000 msec.
        /// </summary>
        public int MaxWaitTime = DefaultMaxBlockingWaitTime;
        
		/// <summary>
        /// Defines how many bytes should be available before returning data. A value of 0 indicate a no blocking command.
        /// </summary>
        public int MinBytes = DefaultMinBlockingByteBufferSize;

        public List<FetchRequestItem> Fetches { get; set; }

        public byte[] Encode()
        {
            return EncodeFetchRequest(this);
        }

        public IEnumerable<FetchResponse> Decode(byte[] payload)
        {
			return FetchResponse.Decode(payload);
        }

        private byte[] EncodeFetchRequest(FetchRequest request)
        {
            var message = new WriteByteStream();
            if (request.Fetches == null) request.Fetches = new List<FetchRequestItem>();

            message.Pack(EncodeHeader(request));

            var topicGroups = request.Fetches.GroupBy(x => x.Topic).ToList();
            message.Pack(ReplicaId.ToBytes(), request.MaxWaitTime.ToBytes(), request.MinBytes.ToBytes(), topicGroups.Count.ToBytes());

            foreach (var topicGroup in topicGroups)
            {
                var partitions = topicGroup.GroupBy(x => x.PartitionId).ToList();
                message.Pack(topicGroup.Key.ToInt16SizedBytes(), partitions.Count.ToBytes());

                foreach (var partition in partitions)
                {
                    foreach (var fetch in partition)
                    {
                        message.Pack(partition.Key.ToBytes(), fetch.Offset.ToBytes(), fetch.MaxBytes.ToBytes());
                    }
                }
            }

            message.Prepend(message.Length().ToBytes());

            return message.Payload();
        }
    }

    public class FetchRequestItem
    {
        public FetchRequestItem()
        {
            MaxBytes = FetchRequest.DefaultMinBlockingByteBufferSize * 8;
        }

        /// <summary>
        /// The name of the topic.
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// The id of the partition the fetch is for.
        /// </summary>
        public int PartitionId { get; set; }
        
		/// <summary>
        /// The offset to begin this fetch from.
        /// </summary>
        public long Offset { get; set; }
        
		/// <summary>
        /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
        /// </summary>
        public int MaxBytes { get; set; }
    }

    
}
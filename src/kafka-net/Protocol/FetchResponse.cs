using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
	public class FetchResponse
	{
		/// <summary>
		/// The name of the topic this response entry is for.
		/// </summary>
		public string Topic { get; set; }
		/// <summary>
		/// The id of the partition this response is for.
		/// </summary>
		public int PartitionId { get; set; }
		/// <summary>
		/// Error code of exception that occured during the request.  Zero if no error.
		/// </summary>
		public Int16 Error { get; set; }
		/// <summary>
		/// The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
		/// </summary>
		public long HighWaterMark { get; set; }

		public List<Message> Messages { get; set; }

		public FetchResponse()
		{
			Messages = new List<Message>();
		}

		internal static IEnumerable<FetchResponse> Decode(byte[] data)
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
					var partitionId = stream.ReadInt();
					var response = new FetchResponse
					{
						Topic = topic,
						PartitionId = partitionId,
						Error = stream.ReadInt16(),
						HighWaterMark = stream.ReadLong()
					};
					//note: dont use initializer here as it breaks stream position.
					response.Messages = MessageSet.Decode(stream.ReadIntPrefixedBytes())
						.Select(x => { x.Meta.PartitionId = partitionId; return x; })
						.ToList();
					yield return response;
				}
			}
		}
	}
}

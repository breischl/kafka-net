using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet.Model
{
	public class ProduceResult
	{
		public ICollection<Message> SuccessfulMessages { get; private set; }
		public IDictionary<Message, Exception> FailedMessages { get; private set; }
		public int MessageCount
		{
			get
			{
				return SuccessfulMessages.Count + FailedMessages.Count;
			}
		}

		internal ProduceResult(int messageCount)
		{
			this.SuccessfulMessages = new List<Message>(messageCount);
			this.FailedMessages = new Dictionary<Message, Exception>();
		}
	}
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet.Protocol;

namespace KafkaNet.Model
{
	public class ProducerOptions
	{
		public const Int16 NO_ACKS = 0;
		public const Int16 MASTER_ACK = 1;
		public const Int16 ALL_NODES_ACK = -1;


		public MessageCodec MessageCodec { get; private set; }
		public TimeSpan SendTimeout { get; private set; }
		public Int16 Acks { get; private set; }
		public TimeSpan MaxAccumulationTime { get; private set; }
		public int? MaxAccumulationMessages { get; private set; }
		public IBrokerRouter Router { get; private set; }

		public ProducerOptions(IBrokerRouter router, MessageCodec codec = MessageCodec.CodecGzip, Int16 acks = MASTER_ACK, TimeSpan? sendTimeout = null, TimeSpan? maxAccumulationTime = null, int? maxAccumulationMessages = 500)
		{
			this.Router = router;
			this.Acks = acks;
			this.MessageCodec = codec;
			this.SendTimeout = sendTimeout ?? TimeSpan.FromSeconds(10);
			this.MaxAccumulationTime = maxAccumulationTime ?? TimeSpan.FromMilliseconds(500);
			this.MaxAccumulationMessages = maxAccumulationMessages;
		}
	}
}

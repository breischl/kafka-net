using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
	public class ProduceResponse
	{
		/// <summary>
		/// The topic the offset came from.
		/// </summary>
		public string Topic { get; set; }

		/// <summary>
		/// The partition the offset came from.
		/// </summary>
		public int PartitionId { get; set; }

		/// <summary>
		/// The offset number to commit as completed.
		/// </summary>
		public Int16 Error { get; set; }

		public long Offset { get; set; }
	}
}

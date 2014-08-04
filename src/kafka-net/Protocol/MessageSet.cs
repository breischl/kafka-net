using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using KafkaNet.Common;

namespace KafkaNet.Protocol
{
	/// <summary>
	/// Payload represents a collection of messages to be posted to a specified Topic on specified Partition.
	/// </summary>
	public class MessageSet
	{
		private static readonly byte[] OFFSET_BYTES = ((Int64)0).ToBytes();

		/// <summary>
		/// Messages in the MessageSet
		/// </summary>
		public IList<Message> Messages { get; set; }

		/// <summary>
		/// Encodes a collection of messages into one byte[].  Encoded in order of list.
		/// </summary>
		/// <param name="messages">The collection of messages to encode together.</param>
		/// <returns>Encoded byte[] representing the collection of messages.</returns>
		public byte[] Encode(MessageCodec codec = MessageCodec.CodecNone)
		{
			/*
			 Message set format (from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets)
			 Note unlike other arrays, this is *not* prefixed with an int32 size
			
			 MessageSet => [Offset MessageSize Message]
						  Offset => int64
						  MessageSize => int32
			 */

			MessageSet encodingSet;
			switch (codec)
			{
				case MessageCodec.CodecGzip:
					encodingSet = CreateGzipCompressedMessageSet(this);
					break;
				case MessageCodec.CodecNone:
					encodingSet = this;
					break;
				default:
					throw new UnsupportedCompressionMethodException();
			}

			var messageSetBytes = new WriteByteStream();

			foreach (var message in encodingSet.Messages)
			{
				var encodedMessage = message.Encode();
				messageSetBytes.Pack(OFFSET_BYTES); //the Offset, which we don't know, so we can set whatever. Using zero because why not.
				messageSetBytes.Pack(encodedMessage.Length.ToBytes());
				messageSetBytes.Pack(encodedMessage);
			}

			return messageSetBytes.Payload();
		}

		private static MessageSet CreateGzipCompressedMessageSet(MessageSet messageSet)
		{
			var uncompressedMessageSetBytes = messageSet.Encode(codec: MessageCodec.CodecNone);
			var gZipBytes = Compression.Zip(uncompressedMessageSetBytes);
			var compressedMessage = new Message
			{
				Attributes = (byte)(0x00 | (ProtocolConstants.AttributeCodeMask & (byte)MessageCodec.CodecGzip)),
				Value = gZipBytes,
			};

			var compressedMsgSet = new MessageSet()
			{
				Messages = new[] { compressedMessage },
			};

			return compressedMsgSet;
		}

		/// <summary>
		/// Decode a byte[] that represents a collection of messages.
		/// </summary>
		/// <param name="messageSet">The byte[] encode as a message set from kafka.</param>
		/// <returns>Enumerable representing stream of messages decoded from byte[]</returns>
		public static IEnumerable<Message> Decode(byte[] messageSet)
		{
			var stream = new ReadByteStream(messageSet);

			while (stream.HasData)
			{
				//if the message set hits against our max bytes wall on the fetch we will have a 1/2 completed message downloaded.
				//the decode should guard against this situation
				if (stream.Available(Message.MIN_MESSAGE_SIZE) == false)
					yield break;

				var offset = stream.ReadLong();
				var messageSize = stream.ReadInt();

				if (stream.Available(messageSize) == false)
					yield break;

				foreach (var message in Message.Decode(offset, stream.ReadBytesFromStream(messageSize)))
				{
					yield return message;
				}
			}
		}
	}

	internal class AnnotatedMessageSet : MessageSet
	{
		/// <summary>
		/// Topic for the message set. Note that Kafka MessageSets actually have no intrinsic Topic, this is just for convenience when sending
		/// </summary>
		public string Topic { get; set; }

		/// <summary>
		/// Partition for the message set. Note that Kafka MessageSets actually have no intrinsic Partition, this is just for convenience when sending
		/// </summary>
		public int Partition { get; set; }
	}
}

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
    /// Message represents the data from a single event occurance.
    /// </summary>
    public class Message
    {
        public const int MIN_MESSAGE_SIZE = 12;
        private static readonly Crc32 Crc32 = new Crc32();

        /// <summary>
        /// Metadata on source offset and partition location for this message.
        /// </summary>
        public MessageMetadata Meta { get; set; }
        
		/// <summary>
        /// This is a version id used to allow backwards compatible evolution of the message binary format.  Reserved for future use.  
        /// </summary>
        public byte MagicNumber { get; set; }
        
		/// <summary>
        /// Attribute value outside message body used for added codec/compression info.
        /// </summary>
        public byte Attributes { get; set; }
        
		/// <summary>
        /// Key value used for routing message to partitions. Can be null.
        /// </summary>
        public byte[] Key { get; set; }
        
		/// <summary>
        /// The message body contents. Can contain a compressed message set.
        /// </summary>
        public byte[] Value { get; set; }

        /// <summary>
        /// Encodes a message object to byte[]
        /// </summary>
        /// <param name="message">Message data to encode.</param>
        /// <returns>Encoded byte[] representation of the message object.</returns>
        /// <remarks>
        /// Format:
        /// Crc (Int32), MagicByte (Byte), Attribute (Byte), Key (Byte[]), Value (Byte[])
        /// </remarks>
        public byte[] Encode()
        {
            var body = new WriteByteStream();

            body.Pack(new[] { this.MagicNumber },
                      new[] { this.Attributes },
					  this.Key.ToIntPrefixedBytes(),
					  this.Value.ToIntPrefixedBytes());

            var crc = Crc32.ComputeHash(body.Payload());
            body.Prepend(crc);

            return body.Payload();
        }

        /// <summary>
        /// Decode messages from a payload and assign it a given kafka offset.
        /// </summary>
        /// <param name="offset">The offset represting the log entry from kafka of this message.</param>
        /// <param name="payload">The byte[] encode as a message from kafka.</param>
        /// <returns>Enumerable representing stream of messages decoded from byte[].</returns>
        /// <remarks>The return type is an Enumerable as the message could be a compressed message set.</remarks>
        public static IEnumerable<Message> Decode(long offset, byte[] payload)
        {
            var crc = payload.Take(4);
            var stream = new ReadByteStream(payload.Skip(4));

            if (crc.SequenceEqual(Crc32.ComputeHash(stream.Payload)) == false)
                throw new FailCrcCheckException("Payload did not match CRC validation.");

            var message = new Message
            {
                Meta = new MessageMetadata { Offset = offset },
                MagicNumber = stream.ReadByte(),
                Attributes = stream.ReadByte(),
                Key = stream.ReadIntPrefixedBytes()
            };

            var codec = (MessageCodec)(ProtocolConstants.AttributeCodeMask & message.Attributes);
            switch (codec)
            {
                case MessageCodec.CodecNone:
                    message.Value = stream.ReadIntPrefixedBytes();
                    yield return message;
                    break;
                case MessageCodec.CodecGzip:
                    var gZipData = stream.ReadIntPrefixedBytes();
					var unzippedData = Compression.Unzip(gZipData);
                    foreach (var m in MessageSet.Decode(unzippedData))
                    {
                        yield return m;
                    }
                    break;
                default:
                    throw new NotSupportedException(string.Format("Codec type of {0} is not supported.", codec));
            }
        }
    }

    /// <summary>
    /// Provides metadata about the message received from the FetchResponse
    /// </summary>
    /// <remarks>
    /// The purpose of this metadata is to allow client applications to track their own offset information about messages received from Kafka.
    /// <see cref="http://kafka.apache.org/documentation.html#semantics"/>
    /// </remarks>
    public class MessageMetadata
    {
        /// <summary>
        /// The log offset of this message as stored by the Kafka server.
        /// </summary>
        public long Offset { get; set; }
        /// <summary>
        /// The partition id this offset is from.
        /// </summary>
        public int PartitionId { get; set; }
    }
}

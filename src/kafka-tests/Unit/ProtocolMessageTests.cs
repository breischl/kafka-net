using System.Linq;
using KafkaNet;
using KafkaNet.Protocol;
using NUnit.Framework;
using kafka_tests.Helpers;
using System.Text;
using KafkaNet.Common;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class ProtocolMessageTests
    {
        [Test]
        [ExpectedException(typeof(FailCrcCheckException))]
        public void DecodeMessageShouldThrowWhenCrcFails()
        {
            var testMessage = new Message
            {
                Key = "test".ToUnsizedBytes(),
                Value = "kafka test message.".ToUnsizedBytes()
            };

            var encoded = testMessage.Encode();
            encoded[0] += 1;
            var result = Message.Decode(0, encoded).First();
        }

        [Test]
        [TestCase("test key", "test message")]
		[TestCase(null, "test message")]
		[TestCase("test key", null)]
		[TestCase(null, null)]
        public void Message_EncodeAndDecode(string key, string value)
        {
            var testMessage = new Message
                {
                    Key = key.ToUnsizedBytes(),
					Value = (value == null ? null : value.ToUnsizedBytes())
                };

            var encoded = testMessage.Encode();
            var result = Message.Decode(0, encoded).First();

            Assert.That(result.Key, Is.EqualTo(testMessage.Key));
            Assert.That(result.Value, Is.EqualTo(testMessage.Value));
        }

		[Test]
		[TestCase(MessageCodec.CodecNone)]
		[TestCase(MessageCodec.CodecGzip)]
		public void MessageSet_EncodeAndDecode(MessageCodec codec)
		{
			var msgSet = new MessageSet()
			{
				Messages = new[]
                {
                    new Message {Value = 0.ToBytes(), Key = 0.ToBytes()},
                    new Message {Value = 1.ToBytes(), Key = 1.ToBytes()},
                    new Message {Value = 2.ToBytes(), Key = 2.ToBytes()}
                }
			};


			var encoded = msgSet.Encode(codec);
			var results = MessageSet.Decode(encoded).ToArray();

			for (int i = 0; i < msgSet.Messages.Count; i++)
			{
				Assert.That(results[i].Key, Is.EqualTo(i.ToBytes()));
				Assert.That(results[i].Value, Is.EqualTo(i.ToBytes()));
			}
		}

        [Test]
        public void EncodeMessageSetEncodesMultipleMessages()
        {
            //expected generated from python library
            var expected = new byte[]
                {
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 45, 70, 24, 62, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 48, 
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 90, 65, 40, 168, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 49, 
					0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 195, 72, 121, 18, 0, 0, 0, 0, 0, 1, 49, 0, 0, 0, 1, 50
                };

            var messages = new[]
                {
                    new Message {Value = "0".ToUnsizedBytes(), Key = "1".ToUnsizedBytes()},
                    new Message {Value = "1".ToUnsizedBytes(), Key = "1".ToUnsizedBytes()},
                    new Message {Value = "2".ToUnsizedBytes(), Key = "1".ToUnsizedBytes()}
                };

			var messageSet = new MessageSet()
			{
				Messages = messages
			};
			var result = messageSet.Encode();

			Assert.That(result, Is.EqualTo(expected));
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            //This message set has a truncated message bytes at the end of it
            var result = MessageSet.Decode(MessageHelper.FetchResponseMaxBytesOverflow).ToList();
            
            Assert.That(result.Count, Is.EqualTo(529));
        }
    }
}

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
                Key = "test".ToBytes(),
                Value = "kafka test message.".ToBytes()
            };

            var encoded = Message.EncodeMessage(testMessage);
            encoded[0] += 1;
            var result = Message.DecodeMessage(0, encoded).First();
        }

        [Test]
        [TestCase("test key", "test message")]
		[TestCase(null, "test message")]
		[TestCase("test key", null)]
		[TestCase(null, null)]
        public void EnsureMessageEncodeAndDecodeAreCompatible(string key, string value)
        {
            var testMessage = new Message
                {
                    Key = key.ToBytes(),
					Value = (value == null ? new byte[0] : value.ToBytes())
                };

            var encoded = Message.EncodeMessage(testMessage);
            var result = Message.DecodeMessage(0, encoded).First();

            Assert.That(result.Key, Is.EqualTo(testMessage.Key));
            Assert.That(result.Value, Is.EqualTo(testMessage.Value));
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
                    new Message {Value = "0".ToBytes(), Key = "1".ToBytes()},
                    new Message {Value = "1".ToBytes(), Key = "1".ToBytes()},
                    new Message {Value = "2".ToBytes(), Key = "1".ToBytes()}
                };

            var result = Message.EncodeMessageSet(messages);

            Assert.That(expected, Is.EqualTo(result));
        }

        [Test]
        public void DecodeMessageSetShouldHandleResponseWithMaxBufferSizeHit()
        {
            //This message set has a truncated message bytes at the end of it
            var result = Message.DecodeMessageSet(MessageHelper.FetchResponseMaxBytesOverflow).ToList();
            
            Assert.That(result.Count, Is.EqualTo(529));
        }
    }
}

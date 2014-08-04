using System;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using kafka_tests.Helpers;
using System.Text;


namespace kafka_tests.Unit
{
	[TestFixture]
	public class CompressionTests
	{
		[Test]
		public void GzipCompression_IsReversible()
		{
			var text = "abcdefghijklmnopqrstuvwxyz";
			var compressed = Compression.Zip(Encoding.UTF8.GetBytes(text));
			var uncompressed = Compression.Unzip(compressed);
			var resultText = Encoding.UTF8.GetString(uncompressed);
			Assert.That(resultText, Is.EqualTo(text));
		}
	}
}

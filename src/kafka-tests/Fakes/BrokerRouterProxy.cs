﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Moq;
using Ninject.MockingKernel.Moq;
using kafka_tests.Fakes;
using System.Threading;
using System.Net;

namespace kafka_tests
{
	public class BrokerRouterProxy
	{
		public const string TestTopic = "UnitTest";

		private readonly MoqMockingKernel _kernel;
		private int _offset0;
		private int _offset1;
		private FakeKafkaConnection _fakeConn0;
		private FakeKafkaConnection _fakeConn1;
		private Mock<IKafkaConnectionFactory> _factoryMock;

		public FakeKafkaConnection BrokerConn0 { get { return _fakeConn0; } }
		public FakeKafkaConnection BrokerConn1 { get { return _fakeConn1; } }
		public Mock<IKafkaConnectionFactory> KafkaConnectionFactory { get { return _factoryMock; } }

		public Func<MetadataResponse> MetadataResponse = () => DefaultMetadataResponse();

		public IPartitionSelector PartitionSelector = new DefaultPartitionSelector();

		public BrokerRouterProxy(MoqMockingKernel kernel)
		{
			_kernel = kernel;

			//setup mock IKafkaConnection
			_fakeConn0 = new FakeKafkaConnection(new Uri("http://localhost:1"));
			_fakeConn0.ProduceResponseFunction = () => new ProduceResponse { Offset = _offset0++, PartitionId = 0, Topic = TestTopic };
			_fakeConn0.MetadataResponseFunction = () => MetadataResponse();
			_fakeConn0.OffsetResponseFunction = () => new OffsetResponse { Offsets = new List<long> { 0, 99 }, PartitionId = 0, Topic = TestTopic };
			_fakeConn0.FetchResponseFunction = () => { Thread.Sleep(50); return null; };

			_fakeConn1 = new FakeKafkaConnection(new Uri("http://localhost:2"));
			_fakeConn1.ProduceResponseFunction = () => new ProduceResponse { Offset = _offset1++, PartitionId = 1, Topic = TestTopic };
			_fakeConn1.MetadataResponseFunction = () => MetadataResponse();
			_fakeConn1.OffsetResponseFunction = () => new OffsetResponse { Offsets = new List<long> { 0, 100 }, PartitionId = 1, Topic = TestTopic };
			_fakeConn1.FetchResponseFunction = () => { Thread.Sleep(50); return null; };

			_factoryMock = _kernel.GetMock<IKafkaConnectionFactory>();
			_factoryMock.Setup(x => x.Create(It.Is<Uri>(uri => uri.Port == 1), It.IsAny<int>())).Returns(() => _fakeConn0);
			_factoryMock.Setup(x => x.Create(It.Is<KafkaEndpoint>(ep => ep.ServerUri.Port == 1), It.IsAny<int>())).Returns(() => _fakeConn0);
			
			_factoryMock.Setup(x => x.Create(It.Is<Uri>(uri => uri.Port == 2), It.IsAny<int>())).Returns(() => _fakeConn1);
			_factoryMock.Setup(x => x.Create(It.Is<KafkaEndpoint>(ep => ep.Endpoint.Port == 2), It.IsAny<int>())).Returns(() => _fakeConn1);
			
			_factoryMock.Setup(x => x.Resolve(It.IsAny<Uri>())).Returns<Uri>((uri) => new KafkaEndpoint(uri, new IPEndPoint(IPAddress.Loopback, uri.Port)));
		}

		public IBrokerRouter Create()
		{
			return new BrokerRouter(new KafkaOptions
			{
				KafkaServerUri = new List<Uri> { new Uri("http://localhost:1"), new Uri("http://localhost:2") },
				KafkaConnectionFactory = _factoryMock.Object,
				PartitionSelector = PartitionSelector
			});
		}

		public static MetadataResponse DefaultMetadataResponse()
		{
			var resp = new MetadataResponse
			{
				CorrelationId = 1,
				Brokers = new List<Broker>
                        {
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 1,
                                    BrokerId = 0
                                },
                            new Broker
                                {
                                    Host = "localhost",
                                    Port = 2,
                                    BrokerId = 1
                                },
                        }
			};

			resp.Topics = Enumerable.Range(-1, 101).Select(i => new Topic
			{
				ErrorCode = 0,
				Name = TestTopic + i,
				Partitions = new List<Partition> {
                            new Partition {
                                    ErrorCode = 0,
                                    Isrs = new List<int> {1},
                                    PartitionId = 0,
                                    LeaderId = 0,
                                    Replicas = new List<int> {1},
                                },
                            new Partition {
                                    ErrorCode = 0,
                                    Isrs = new List<int> {1},
                                    PartitionId = 1,
                                    LeaderId = 1,
                                    Replicas = new List<int> {1},
                                }
				}
			}).ToList();

			resp.Topics.First().Name = TestTopic;

			return resp;
		}
	}
}

﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using KafkaNet;
using KafkaNet.Model;
using NUnit.Framework;
using KafkaNet.Protocol;
using KafkaNet.Common;

namespace kafka_tests.Unit
{
    [TestFixture]
    [Category("Unit")]
    public class DefaultPartitionSelectorTests
    {
        private Topic _topicA;
        private Topic _topicB;

        [SetUp]
        public void Setup()
        {
            _topicA = new Topic
            {
                Name = "a",
                Partitions = new List<Partition>(new[]
                {
                    new Partition
                        {
                            LeaderId = 0,
                            PartitionId = 0
                        },
                    new Partition
                        {
                            LeaderId = 1,
                            PartitionId = 1
                        }
                })
            };

            _topicB = new Topic
            {
                Name = "b",
                Partitions = new List<Partition>(new[]
                {
                    new Partition
                        {
                            LeaderId = 0,
                            PartitionId = 0
                        },
                    new Partition
                        {
                            LeaderId = 1,
                            PartitionId = 1
                        }
                })
            };
        }

        [Test]
        public void RoundRobinShouldRollOver()
        {
            var selector = new DefaultPartitionSelector();

            var first = selector.Select(_topicA, null);
            var second = selector.Select(_topicA, null);
            var third = selector.Select(_topicA, null);

            Assert.That(first.PartitionId, Is.EqualTo(0));
            Assert.That(second.PartitionId, Is.EqualTo(1));
            Assert.That(third.PartitionId, Is.EqualTo(0));
        }

        [Test]
        public void RoundRobinShouldHandleMultiThreadedRollOver()
        {
            var selector = new DefaultPartitionSelector();
            var bag = new ConcurrentBag<Partition>();

            Parallel.For(0, 100, x => bag.Add(selector.Select(_topicA, null)));

            Assert.That(bag.Count(x => x.PartitionId == 0), Is.EqualTo(50));
            Assert.That(bag.Count(x => x.PartitionId == 1), Is.EqualTo(50));
        }

        [Test]
        public void RoundRobinShouldTrackEachTopicSeparately()
        {
            var selector = new DefaultPartitionSelector();

            var a1 = selector.Select(_topicA, null);
            var b1 = selector.Select(_topicB, null);
            var a2 = selector.Select(_topicA, null);
            var b2 = selector.Select(_topicB, null);

            Assert.That(a1.PartitionId, Is.EqualTo(0));
            Assert.That(a2.PartitionId, Is.EqualTo(1));

            Assert.That(b1.PartitionId, Is.EqualTo(0));
            Assert.That(b2.PartitionId, Is.EqualTo(1));
        }

        [Test]
        public void KeyHashShouldSelectEachPartitionType()
        {
            var selector = new DefaultPartitionSelector();

            var first = selector.Select(_topicA, "0".ToBytes());
            var second = selector.Select(_topicA, "1".ToBytes());

            Assert.That(first.PartitionId, Is.EqualTo(0));
            Assert.That(second.PartitionId, Is.EqualTo(1));
        }

        [Test]
        [ExpectedException(typeof(InvalidPartitionException))]
        public void KeyHashShouldThrowExceptionWhenChoosesAPartitionIdThatDoesNotExist()
        {
            var selector = new DefaultPartitionSelector();
            var list = new List<Partition>(_topicA.Partitions);
            list[1].PartitionId = 999;
            var topic = new Topic
                {
                    Name = "badPartition",
                    Partitions = list
                };


            selector.Select(topic, "1".ToBytes());
        }

        [Test]
        [ExpectedException(typeof(ApplicationException))]
        public void SelectorShouldThrowExceptionWhenPartitionsAreEmpty()
        {
            var selector = new DefaultPartitionSelector();
            var topic = new Topic
            {
                Name = "emptyPartition",
                Partitions = new List<Partition>()
            };
            selector.Select(topic, "1".ToBytes());
        }

    }
}

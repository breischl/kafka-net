﻿using System;
using System.Collections.Concurrent;
using System.Linq;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class DefaultPartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, Partition> _roundRobinTracker = new ConcurrentDictionary<string, Partition>();
        public Partition Select(Topic topic, byte[] key)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            if (topic.Partitions.Count <= 0) throw new ApplicationException(string.Format("Topic ({0}) has no partitions.", topic.Name));
            
            //use round robing
            var partitions = topic.Partitions;
            if (key == null)
            {
                return _roundRobinTracker.AddOrUpdate(topic.Name, x => partitions.First(), (s, i) =>
                    {
                        var index = partitions.FindIndex(0, p => p.Equals(i));
                        if (index == -1) return partitions.First();
                        if (++index >= partitions.Count) return partitions.First();
                        return partitions[index];
                    });
            }
            
            //use key hash
			//TODO: This seems kind of bad, as it will give an inconsistent hash. 
			//Perhaps it would be better to MD5 the bytes and use that, or something along those lines?
            var partitionId = Math.Abs(key.GetHashCode()) % partitions.Count;
            var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);

            if (partition == null)
                throw new InvalidPartitionException(string.Format("Hash function return partition id: {0}, but the available partitions are:{1}",
                                                                            partitionId, string.Join(",", partitions.Select(x => x.PartitionId))));

            return partition;
        }
    }
}
using System;
using System.Collections.Concurrent;
using System.Linq;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class DefaultPartitionSelector : IPartitionSelector
    {
        private readonly ConcurrentDictionary<string, int> _roundRobinTracker = new ConcurrentDictionary<string, int>();
        public Partition Select(Topic topic, byte[] key)
        {
            if (topic == null) throw new ArgumentNullException("topic");
            if (topic.Partitions.Count <= 0) throw new ApplicationException(string.Format("Topic ({0}) has no partitions.", topic.Name));
            
            var partitions = topic.Partitions;
			if (key == null)
			{
				//use round robin-ing
				var partitionIdx = _roundRobinTracker.AddOrUpdate(topic.Name, x => 0, (s, i) =>
					{
						return ((i + 1) % partitions.Count);
					});

				return partitions[partitionIdx];
			}
			else
			{
				//use key hash
				//TODO: We're using an arbitrarily chosen hash function here. Might be useful/necessary to make this pluggable so that it can match what other clients do. 
				var partitionId = Math.Abs(ComputeHashCode(key)) % partitions.Count;
				var partition = partitions.FirstOrDefault(x => x.PartitionId == partitionId);

				if (partition == null)
				{
					throw new InvalidPartitionException(string.Format("Hash function return partition id: {0}, but the available partitions are:{1}",
																				partitionId, string.Join(",", partitions.Select(x => x.PartitionId))));
				}

				return partition;
			}
        }

		private int ComputeHashCode(byte[] key)
		{
			//Quick-and-dirty FNV Hash method
			//Lifted from http://stackoverflow.com/a/468084/76263
			unchecked
			{
				const int p = 16777619;
				int hash = (int)2166136261;

				for (int i = 0; i < key.Length; i++)
				{
					hash = (hash ^ key[i]) * p;
				}

				hash += hash << 13;
				hash ^= hash >> 7;
				hash += hash << 3;
				hash ^= hash >> 17;
				hash += hash << 5;
				return hash;
			}
		}
    }
}
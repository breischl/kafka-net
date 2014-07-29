﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaNet.Model
{
    public class KafkaOptions
    {
        private const int DefaultResponseTimeout = 30000;

        /// <summary>
        /// List of Uri connections to kafka servers.  The are used to query for metadata from Kafka.  More than one is recommended.
        /// </summary>
        public List<Uri> KafkaServerUri { get; set; }
        
		/// <summary>
        /// Provides a factory for creating new kafka connections.
        /// </summary>
        public IKafkaConnectionFactory KafkaConnectionFactory { get; set; }
        
		/// <summary>
        /// Selector function for routing messages to partitions. Default is key/hash and round robin.
        /// </summary>
        public IPartitionSelector PartitionSelector { get; set; }
        
		/// <summary>
        /// Timeout length in milliseconds waiting for a response from kafka.
        /// </summary>
        public int ResponseTimeoutMs { get; set; }
        
        public KafkaOptions(params Uri[] kafkaServerUri)
        {
            KafkaServerUri = kafkaServerUri.ToList();
            KafkaConnectionFactory = new DefaultKafkaConnectionFactory();
            PartitionSelector = new DefaultPartitionSelector();
            ResponseTimeoutMs = DefaultResponseTimeout;
        }
    }
}

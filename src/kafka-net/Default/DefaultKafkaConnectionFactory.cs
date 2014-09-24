using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using Common.Logging;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    public class DefaultKafkaConnectionFactory : IKafkaConnectionFactory
    {
		private static readonly ILog _log = LogManager.GetLogger<DefaultKafkaConnectionFactory>();

        public IKafkaConnection Create(KafkaEndpoint endpoint, int responseTimeoutMs)
        {
            return new KafkaConnection(new KafkaTcpSocket(endpoint), responseTimeoutMs);
        }

		public IKafkaConnection Create(Uri kafkaAddress, int responseTimeoutMs)
		{
			var endpoint = Resolve(kafkaAddress);
			return Create(endpoint, responseTimeoutMs);
		}

        public KafkaEndpoint Resolve(Uri kafkaAddress)
        {
            var ipAddress = GetFirstAddress(kafkaAddress.Host);
            var ipEndpoint = new IPEndPoint(ipAddress, kafkaAddress.Port);

            var kafkaEndpoint = new KafkaEndpoint(kafkaAddress, ipEndpoint);
            return kafkaEndpoint;
        }

        private static IPAddress GetFirstAddress(string hostname)
        {
            //lookup the IP address from the provided host name
            var addresses = Dns.GetHostAddresses(hostname);

            if (addresses.Length > 0)
            {
				if (_log.IsDebugEnabled)
				{
					Array.ForEach(addresses, address => _log.DebugFormat("Found address {0} for {1}", addresses, hostname));
				}

                var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();

                _log.DebugFormat("Using address {0} for {1}", selectedAddress, hostname);

                return selectedAddress;
            }

            throw new UnresolvedHostnameException("Could not resolve the following hostname: {0}", hostname);
        }
    }
}

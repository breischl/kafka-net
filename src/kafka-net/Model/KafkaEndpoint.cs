using System;
using System.Net;

namespace KafkaNet.Model
{
    public class KafkaEndpoint
    {
		public Uri ServerUri { get; private set; }
		public IPEndPoint Endpoint { get; private set; }

		internal KafkaEndpoint(Uri serverUri, IPEndPoint endpoint)
		{
			if (serverUri == null)
			{
				throw new ArgumentNullException("serverUri");
			}
			if (endpoint == null)
			{
				throw new ArgumentNullException("endpoint");
			}

			this.ServerUri = serverUri;
			this.Endpoint = endpoint;
		}

        protected bool Equals(KafkaEndpoint other)
        {
			if (other == null)
			{
				return false;
			}
			else
			{
				return this.Endpoint.Equals(other.Endpoint);
			}
        }

        public override int GetHashCode()
        {
			return Endpoint.GetHashCode();
        }

		public override bool Equals(object obj)
		{
			KafkaEndpoint ke = obj as KafkaEndpoint;
			return this.Equals(ke);
		}

        public override string ToString()
        {
            return ServerUri.ToString();
        }
    }
}

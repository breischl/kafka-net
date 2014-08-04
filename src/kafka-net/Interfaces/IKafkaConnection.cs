using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KafkaNet
{
    public interface IKafkaConnection : IDisposable
    {
        Uri KafkaUri { get; }
        Task<List<T>> SendAsync<T>(IKafkaRequest<T> request);
		bool IsOpen { get; }
		int ConnectionId { get; }
    }
}

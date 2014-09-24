using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Protocol;
using Common.Logging;
using KafkaNet.Model;

namespace KafkaNet
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled publicly.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket, IDisposable
    {
        private readonly CancellationTokenSource _disposeTokenSource = new CancellationTokenSource();
		private static readonly ILog _log = LogManager.GetLogger<KafkaTcpSocket>();
        private readonly KafkaEndpoint _endpoint;

		private readonly object _clientLock = new object();
        private TcpClient _client;
		private NetworkStream _stream;
		
        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="serverUri">The server to connect to.</param>
        /// <param name="delayConnectAttemptMS">Time in milliseconds to delay the initial connection attempt to the given server.</param>
		public KafkaTcpSocket(KafkaEndpoint endpoint)
        {
			_endpoint = endpoint;
        }

        /// <summary>
        /// The Uri to the connected server.
        /// </summary>
		public Uri ServerUri { get { return _endpoint.ServerUri; } }

		public KafkaEndpoint Endpoint { get { return _endpoint; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
		public Task<byte[]> ReadAsync(int readSize)
		{
			return ReadAsync(readSize, CancellationToken.None);
		}

		/// <summary>
		/// Read a certain byte array size return only when all bytes received.
		/// </summary>
		/// <param name="readSize">The size in bytes to receive from server.</param>
		/// <param name="cancelToken">A cancellation token which will cancel the request.</param>
		/// <returns>Returns a byte[] array with the size of readSize.</returns>
		public async Task<byte[]> ReadAsync(int readSize, CancellationToken cancelToken)
		{
			var _myCancelSource = CancellationTokenSource.CreateLinkedTokenSource(_disposeTokenSource.Token, cancelToken);

			try
			{
				EnsureConnected();
				var bytesRead = 0;
				var readBuffer = new byte[readSize];
				while (bytesRead < readSize)
				{
					_myCancelSource.Token.ThrowIfCancellationRequested();
					var bytesReceived = await _stream.ReadAsync(readBuffer, bytesRead, readSize - bytesRead, _myCancelSource.Token);
					bytesRead += bytesReceived;

					if (bytesRead == 0)
					{
						throw new ServerDisconnectedException("Server " + ServerUri.AbsoluteUri + " disconnected in read loop");
					}
				}

				return readBuffer;
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (ObjectDisposedException ode)
			{
				if (_disposeTokenSource.IsCancellationRequested)
				{
					throw new ServerDisconnectedException("Server" + ServerUri.AbsoluteUri + " disconnected during read", ode);
				}
				else
				{
					throw;
				}
			}
			catch (Exception)
			{
				Disconnect();
				throw;
			}
		}

		        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
		public Task WriteAsync(byte[] buffer)
		{
			return WriteAsync(buffer, CancellationToken.None);
		}

        /// <summary>
        /// Write the buffer data to the server.
        /// </summary>
        /// <param name="buffer">The buffer data to send.</param>
		/// <param name="cancelToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public async Task WriteAsync(byte[] buffer, CancellationToken cancelToken)
        {
			var _myCancelSource = CancellationTokenSource.CreateLinkedTokenSource(_disposeTokenSource.Token, cancelToken);

			try
			{
				_myCancelSource.Token.ThrowIfCancellationRequested();
				EnsureConnected();
				_myCancelSource.Token.ThrowIfCancellationRequested();
				await _stream.WriteAsync(buffer, 0, buffer.Length, _myCancelSource.Token);
			}
			//catch (OperationCanceledException)
			//{
			//	throw;
			//}
			catch (ObjectDisposedException ode)
			{
				if (_disposeTokenSource.IsCancellationRequested)
				{
					throw new ServerDisconnectedException("Server" + ServerUri.AbsoluteUri + " disconnected during write", ode);
				}
				else
				{
					throw;
				}
			}
			catch (Exception)
			{
				Disconnect();
				throw;
			}
        }

		private void EnsureConnected()
		{
			if (_stream != null && _stream.CanRead)
			{
				return;
			}

			lock (_clientLock)
			{
				if (_client == null)
				{
					_client = new TcpClient(ServerUri.Host, ServerUri.Port);
				}

				if (!_client.Connected)
				{
					_client.Connect(ServerUri.Host, ServerUri.Port);
				}

				if (_stream == null)
				{
					_stream = _client.GetStream();
				}
			}
		}

		private void Disconnect()
		{
			lock (_clientLock)
			{
				if (_client != null)
				{
					_client.Close();
					_client = null;
				}

				if (_stream != null)
				{
					_stream.Close();
					_stream.Dispose();
					_stream = null;
				}
			}
		}

        public void Dispose()
        {
			this.Dispose(true);
        }

		protected virtual void Dispose(bool disposing)
		{
			if (disposing)
			{
				_disposeTokenSource.Cancel();
				Disconnect();
			}
		}
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using System.Threading;
using KafkaNet.Common;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// The TcpSocket provides an abstraction from the main driver from having to handle connection to and reconnections with a server.
    /// The interface is intentionally limited to only read/write.  All connection and reconnect details are handled publicly.
    /// </summary>
    public class KafkaTcpSocket : IKafkaTcpSocket, IDisposable
    {
        private readonly CancellationTokenSource _disposeTokenSource = new CancellationTokenSource();
        private readonly IKafkaLog _log;
        private readonly Uri _serverUri;

		private readonly object _clientLock = new object();
        private TcpClient _client;
		private NetworkStream _stream;
		
        /// <summary>
        /// Construct socket and open connection to a specified server.
        /// </summary>
        /// <param name="log">Logging facility for verbose messaging of actions.</param>
        /// <param name="serverUri">The server to connect to.</param>
        /// <param name="delayConnectAttemptMS">Time in milliseconds to delay the initial connection attempt to the given server.</param>
		public KafkaTcpSocket(IKafkaLog log, Uri serverUri)
        {
            _log = log;
            _serverUri = serverUri;
        }

        /// <summary>
        /// The Uri to the connected server.
        /// </summary>
		public Uri ServerUri { get { return _serverUri; } }

        /// <summary>
        /// Read a certain byte array size return only when all bytes received.
        /// </summary>
        /// <param name="readSize">The size in bytes to receive from server.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns a byte[] array with the size of readSize.</returns>
		public async Task<byte[]> ReadAsync(int readSize)
		{
			try
			{
				EnsureConnected();
				var bytesRead = 0;
				var readBuffer = new byte[readSize];
				while (bytesRead < readSize)
				{
					_disposeTokenSource.Token.ThrowIfCancellationRequested();
					var bytesReceived = await _stream.ReadAsync(readBuffer, bytesRead, readSize - bytesRead);
					bytesRead += bytesReceived;

					if (bytesRead == 0)
					{
						throw new ServerDisconnectedException("Server " + _serverUri + " disconnected");
					}
				}

				return readBuffer;
			}
			catch (TaskCanceledException)
			{
				throw;
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (ObjectDisposedException)
			{
				if (_disposeTokenSource.IsCancellationRequested)
				{
					throw new OperationCanceledException();
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
        /// <param name="offset">The offset to start the read from the buffer.</param>
        /// <param name="count">The length of data to read off the buffer.</param>
        /// <param name="cancellationToken">A cancellation token which will cancel the request.</param>
        /// <returns>Returns Task handle to the write operation.</returns>
        public async Task WriteAsync(byte[] buffer)
        {
			try
			{
				EnsureConnected();
				_disposeTokenSource.Token.ThrowIfCancellationRequested();
				await _stream.WriteAsync(buffer, 0, buffer.Length);
			}
			catch (TaskCanceledException)
			{
				throw;
			}
			catch (OperationCanceledException)
			{
				throw;
			}
			catch (ObjectDisposedException)
			{
				if (_disposeTokenSource.IsCancellationRequested)
				{
					throw new OperationCanceledException();
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
					_client = new TcpClient(_serverUri.Host, _serverUri.Port);
				}

				if (!_client.Connected)
				{
					_client.Connect(_serverUri.Host, _serverUri.Port);
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
				if (_stream != null)
				{
					_stream.Dispose();
					_stream = null;
				}

				if (_client != null)
				{
					_client.Close();
					_client = null;
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

﻿using System;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Common;

namespace kafka_tests.Helpers
{
    public class FakeTcpServer : IDisposable
    {
        public delegate void BytesReceivedDelegate(byte[] data);
        public delegate void ClientEventDelegate();
        public event BytesReceivedDelegate OnBytesReceived;
        public event ClientEventDelegate OnClientConnected;
        public event ClientEventDelegate OnClientDisconnected;

        private readonly SemaphoreSlim _serverCloseSemaphore = new SemaphoreSlim(0);

        private TcpClient _client;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private readonly ThreadWall _threadWall = new ThreadWall(ThreadWallInitialState.Blocked);
        private readonly TcpListener _listener;

        public int ConnectionEventcount = 0;
        public int DisconnectionEventCount = 0;

        public FakeTcpServer(int port)
        {
            _listener = new TcpListener(IPAddress.Any, port);
            _listener.Start();

            OnClientConnected += () => Interlocked.Increment(ref ConnectionEventcount);
            OnClientDisconnected += () => Interlocked.Increment(ref DisconnectionEventCount);

            StartHandlingClientRequestAsync();
        }

        public async Task SendDataAsync(byte[] data)
        {
            await _threadWall.RequestPassageAsync();
            Console.WriteLine("FakeTcpServer: writing {0} bytes.", data.Length);
            await _client.GetStream().WriteAsync(data, 0, data.Length);
        }

        public Task SendDataAsync(string data)
        {
            var msg = Encoding.ASCII.GetBytes(data);
            return SendDataAsync(msg);
        }

        public void DropConnection()
        {
            if (_client != null)
            {
				_client.GetStream().Dispose();
				_client.Close();

                _client = null;
            }
        }

        private async Task StartHandlingClientRequestAsync()
        {
            while (_disposeToken.IsCancellationRequested == false)
            {
                Console.WriteLine("FakeTcpServer: Accepting clients.");
                _client = await _listener.AcceptTcpClientAsync();

                Console.WriteLine("FakeTcpServer: Connected client");
                if (OnClientConnected != null) OnClientConnected();
                _threadWall.Release();

                try
                {
                    using (_client)
                    {
                        var buffer = new byte[4096];
                        var stream = _client.GetStream();

                        while (!_disposeToken.IsCancellationRequested)
                        {
                            //connect client
                            var connectTask = stream.ReadAsync(buffer, 0, buffer.Length, _disposeToken.Token);
                            
                            var bytesReceived = await connectTask;
                            if (bytesReceived > 0)
                            {
                                if (OnBytesReceived != null) OnBytesReceived(buffer.Take(bytesReceived).ToArray());
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("FakeTcpServer: Client exception...  Exception:{0}", ex.Message);
                }
                finally
                {
                    Console.WriteLine("FakeTcpServer: Client Disconnected.");
                    _threadWall.Block();
                    if (OnClientDisconnected != null) OnClientDisconnected();
                }
            }
        }

        public void Dispose()
        {
            if (_disposeToken != null) _disposeToken.Cancel();
            _listener.Stop();

            using (_disposeToken)
            using (_serverCloseSemaphore)
            {
                _serverCloseSemaphore.Release();
            }
        }
    }


}

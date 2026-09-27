using Alor.OpenAPI.Interfaces;
using Alor.OpenAPI.Managers;
using Alor.OpenAPI.Websocket;
using Moq;
using Serilog;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;

namespace Alor.OpenAPI.Tests
{
    public class WebSocketReconnectTimeoutTests
    {
        private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(200);
        private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(10);

        [Fact]
        public async Task Restart_ConnectHangsOnFirstAttempt_RetriesAndReconnects()
        {
            var initial = new FakeWebSocketClient();
            var hungConnect = new FakeWebSocketClient { ConnectBehavior = _ => new TaskCompletionSource().Task };
            var healthy = new FakeWebSocketClient();
            var (manager, info, loggerMock) = CreateManager(initial, hungConnect, healthy);
            manager.AddToOpcodes("guid", "subscribe JwtToken");

            Assert.True(await manager.SendOrStartAndSend("hello"));
            initial.Drop();

            Assert.True(await WaitUntil(() => info.IsConnected && healthy.Sent.Contains("subscribe testToken")),
                "Reconnect did not complete after a connect attempt hung.");
            var details = manager.GetSocketInfoDetails();
            Assert.Equal(2, details.ReconnectCount);
            Assert.True(details.LastReconnectSuccessUtc > details.LastReconnectStartUtc);
            Assert.True(hungConnect.Disposed);
            loggerMock.Verify(l => l.Warning(It.Is<string>(m =>
                m.Contains("Попытка переподключения 1 не удалась (TimeoutException)"))), Times.Once());
            Assert.True(await WaitUntil(() => loggerMock.Invocations.Any(i =>
                i.Method.Name == nameof(ILogger.Information) &&
                ((string)i.Arguments[0]).Contains("Переподключение успешно"))));
        }

        [Fact]
        public async Task Restart_CloseHandshakeHangs_RestartStillProceeds()
        {
            var hungClose = new FakeWebSocketClient { CloseBehavior = _ => new TaskCompletionSource().Task };
            var healthy = new FakeWebSocketClient();
            var (manager, info, _) = CreateManager(hungClose, healthy);
            manager.AddToOpcodes("guid", "subscribe JwtToken");

            Assert.True(await manager.SendOrStartAndSend("hello"));
            hungClose.Drop();

            Assert.True(await WaitUntil(() => info.IsConnected && healthy.Sent.Contains("subscribe testToken")),
                "Reconnect did not proceed after the close handshake hung.");
            Assert.Equal(1, info.ReconnectCount);
            Assert.True(hungClose.Disposed);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task StaleLoopFinisher_DoesNotTearDownOrSilenceNewConnection(bool finishesDuringConnect)
        {
            var old = new FakeWebSocketClient();
            var current = new FakeWebSocketClient
            {
                ConnectGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
            };
            var clients = new Queue<FakeWebSocketClient>([old, current]);
            var info = new WebSocketInfo(1, "TestSocket", clients.Dequeue)
            {
                ConnectTimeout = WaitLimit,
                CloseTimeout = ShortTimeout,
                SendTimeout = ShortTimeout,
            };
            var closes = 0;
            var errors = 0;

            await info.StartAsync();
            var oldCts = (CancellationTokenSource)typeof(WebSocketInfo)
                .GetField("_cts", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(info)!;
            await info.CloseSocketAndResetCounters();

            info.Closed = _ =>
            {
                Interlocked.Increment(ref closes);
                return Task.CompletedTask;
            };
            info.Error = (_, _) =>
            {
                Interlocked.Increment(ref errors);
                return Task.CompletedTask;
            };

            var start = info.StartAsync();
            if (!finishesDuringConnect)
            {
                current.ConnectGate.SetResult();
                await start.WaitAsync(WaitLimit);
            }

            // A loop of the replaced connection finishes late (thread-pool scheduling race).
            var finisher = typeof(WebSocketInfo).GetMethod("Finisher", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var stale = (Task)finisher.Invoke(info, [Task.CompletedTask, oldCts, old])!;
            await Task.WhenAny(stale, Task.Delay(100));

            current.ConnectGate.TrySetResult();
            Assert.True(await WaitUntil(() => start.IsCompleted), "StartAsync did not complete.");
            Assert.True(info.IsConnected);
            Assert.Equal(0, Volatile.Read(ref closes) + Volatile.Read(ref errors));

            current.Drop();
            Assert.True(await WaitUntil(() => Volatile.Read(ref errors) == 1),
                "Loss of the new connection was not reported.");
            info.Dispose();
        }

        [Fact]
        public async Task SendAsync_HungSend_ThrowsTimeout()
        {
            var client = new FakeWebSocketClient { SendBehavior = _ => new TaskCompletionSource().Task };
            var info = new WebSocketInfo(1, "TestSocket", () => client) { SendTimeout = ShortTimeout };
            await info.StartAsync();

            var send = info.SendAsync("subscribe");

            Assert.True(await WaitUntil(() => send.IsCompleted), "Send did not time out.");
            await Assert.ThrowsAsync<TimeoutException>(() => send);
            info.Dispose();
        }

        private static (WebSocketConnectionManager manager, WebSocketInfo info, Mock<ILogger> loggerMock)
            CreateManager(params FakeWebSocketClient[] clients)
        {
            var loggerMock = new Mock<ILogger>();
            var metricsRegistryMock = new Mock<IMetricsRegistry>();
            metricsRegistryMock.Setup(x => x.MetricsOptions).Returns(new ConcurrentDictionary<string, object>());
            var manager = new WebSocketConnectionManager(loggerMock.Object, new Uri("wss://test.websocket.com"),
                "testToken", metricsRegistryMock.Object, () => { }, () => { }, 1, "TestSocket", (_, _) => { });

            var queue = new ConcurrentQueue<FakeWebSocketClient>(clients);
            var info = new WebSocketInfo(1, "TestSocket",
                () => queue.TryDequeue(out var next) ? next : throw new InvalidOperationException("no more clients"))
            {
                ConnectTimeout = ShortTimeout,
                CloseTimeout = ShortTimeout,
                SendTimeout = ShortTimeout,
            };
            typeof(WebSocketConnectionManager)
                .GetField("_webSocketInfo", BindingFlags.NonPublic | BindingFlags.Instance)!
                .SetValue(manager, info);

            return (manager, info, loggerMock);
        }

        private static async Task<bool> WaitUntil(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + WaitLimit;
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                    return true;
                await Task.Delay(20);
            }

            return condition();
        }

        private sealed class FakeWebSocketClient : IWebSocketClient
        {
            private readonly TaskCompletionSource _dropped = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private volatile int _state = (int)WebSocketState.None;

            public Func<CancellationToken, Task> ConnectBehavior { get; init; } = _ => Task.CompletedTask;
            public Func<CancellationToken, Task> CloseBehavior { get; init; } = _ => Task.CompletedTask;
            public Func<CancellationToken, Task> SendBehavior { get; init; } = _ => Task.CompletedTask;
            public TaskCompletionSource ConnectGate { get; init; } = CompletedGate();
            public ConcurrentQueue<string> Sent { get; } = new();
            public bool Disposed { get; private set; }

            public WebSocketState State => (WebSocketState)_state;

            public async Task ConnectAsync(CancellationToken cancellationToken)
            {
                _state = (int)WebSocketState.Connecting;
                await ConnectGate.Task;
                await ConnectBehavior(cancellationToken);
                _state = (int)WebSocketState.Open;
            }

            public async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer,
                CancellationToken cancellationToken)
            {
                await _dropped.Task.WaitAsync(cancellationToken);
                throw new InvalidOperationException("unreachable");
            }

            public Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage,
                CancellationToken cancellationToken)
            {
                Sent.Enqueue(Encoding.UTF8.GetString(buffer));
                return SendBehavior(cancellationToken);
            }

            public Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription,
                CancellationToken cancellationToken) => CloseBehavior(cancellationToken);

            public void Drop()
            {
                _state = (int)WebSocketState.Aborted;
                _dropped.TrySetException(new WebSocketException("connection dropped"));
            }

            public void Dispose()
            {
                Disposed = true;
                _state = (int)WebSocketState.Closed;
            }

            private static TaskCompletionSource CompletedGate()
            {
                var gate = new TaskCompletionSource();
                gate.SetResult();
                return gate;
            }
        }
    }
}

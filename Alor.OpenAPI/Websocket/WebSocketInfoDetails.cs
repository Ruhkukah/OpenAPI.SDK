namespace Alor.OpenAPI.Websocket
{
    public sealed class WebSocketInfoDetails
    {
        public WebSocketInfoDetails(
            string name,
            long sentCount,
            long receivedCount,
            DateTime? lastUpdate,
            int reconnectCount,
            long receiveRate,
            double recieveBufferCount,
            long sentRate,
            bool isConnected = false,
            DateTime? lastDisconnectUtc = null,
            DateTime? lastReconnectStartUtc = null,
            DateTime? lastReconnectSuccessUtc = null,
            long? lastDowntimeMs = null)
        {
            Name = name;
            SentCount = sentCount;
            ReceivedCount = receivedCount;
            LastUpdate = lastUpdate;
            ReconnectCount = reconnectCount;
            ReceiveRate = receiveRate;
            RecieveBufferCount = recieveBufferCount;
            SentRate = sentRate;
            IsConnected = isConnected;
            LastDisconnectUtc = lastDisconnectUtc;
            LastReconnectStartUtc = lastReconnectStartUtc;
            LastReconnectSuccessUtc = lastReconnectSuccessUtc;
            LastDowntimeMs = lastDowntimeMs;
        }

        public string Name { get; }
        public long SentCount { get; }
        public long ReceivedCount { get; }
        public DateTime? LastUpdate { get; }
        public int ReconnectCount { get; }
        public long SentRate { get; }
        public long ReceiveRate { get; }
        public double RecieveBufferCount { get; }
        public bool IsConnected { get; }
        public DateTime? LastDisconnectUtc { get; }
        public DateTime? LastReconnectStartUtc { get; }
        public DateTime? LastReconnectSuccessUtc { get; }
        public long? LastDowntimeMs { get; }
    }
}

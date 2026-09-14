using Alor.OpenAPI.Models;
using Alor.OpenAPI.Websocket;

namespace Alor.OpenAPI.Interfaces
{
    public interface IWebSocketsPoolManager : IDisposable
    {
        ISubscriptionManager Subscriptions { get; }
        ICwsManager CommandWs { get; }
        IEnumerable<WebSocketInfoDetails> GetWebSocketsInfoDetail();
    }

    internal interface IInternalWebSocketsPoolManagerActions : IDisposable
    {
        void JwtUpdate(string? newToken);
        IEnumerable<WebSocketInfoDetails> GetWebSocketsInfoDetail();
        void CalculateWebSocketsInfoSentRecieveRates();
        void SetWsResponseMessageHandler(Action<WsResponseMessage>? handler);
        void SetWsResponseCommandMessageHandler(Action<WsResponseCommandMessage>? handler);
        void SetRawWireMessageHandler(Action<WsRawWireMessage>? handler);
        void SetWsParseFailureHandler(Action<WsParseFailure>? handler);
        void SetRawCwsCommandMessageHandler(Action<CwsRawCommandMessage>? handler);
    }
}

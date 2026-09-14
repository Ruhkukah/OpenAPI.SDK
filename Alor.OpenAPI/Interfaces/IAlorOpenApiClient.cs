using Alor.OpenAPI.Models;
using Alor.OpenAPI.Utilities;

namespace Alor.OpenAPI.Interfaces;

public interface IAlorOpenApiClient : IDisposable
{
    IWebSocketsPoolManager WsPoolManager { get; }
    IInstrumentsService Instruments { get; }
    IClientInfoService ClientInfo { get; }
    IOthersService Others { get; }
    IOrdersService Orders { get; }
    IStopOrdersService StopOrders { get; }
    IOrderGroupsService OrderGroups { get; }

    void EnableMetricsCollection();
    void DisableMetricsCollection();


    /// <summary>
    /// Устанавливает обработчик WS-сообщений пользователя (основной поток).
    /// </summary>
    void SetWsResponseMessageHandler(Action<WsResponseMessage>? handler);

    /// <summary>
    /// Устанавливает обработчик WS-командных сообщений пользователя.
    /// </summary>
    void SetWsResponseCommandMessageHandler(Action<WsResponseCommandMessage>? handler);

    /// <summary>
    /// Устанавливает обработчик сырых входящих WS/CWS сообщений до пользовательской десериализации.
    /// </summary>
    void SetRawWireMessageHandler(Action<WsRawWireMessage>? handler);

    /// <summary>
    /// Sets a handler for incoming WS messages that could not be deserialized.
    /// The notification contains routing metadata, never the message payload.
    /// </summary>
    void SetWsParseFailureHandler(Action<WsParseFailure>? handler);

    /// <summary>
    /// Устанавливает обработчик сырых исходящих CWS-команд.
    /// </summary>
    void SetRawCwsCommandMessageHandler(Action<CwsRawCommandMessage>? handler);

    IWebSocketsPoolManager CreateWsPool(IReadOnlyList<string>? names = null, string? commandSocketName = null,
        int sockets = 1, AlorOpenApiLogLevel logLevel = AlorOpenApiLogLevel.Error, string? logFileNameSuffix = null);
}

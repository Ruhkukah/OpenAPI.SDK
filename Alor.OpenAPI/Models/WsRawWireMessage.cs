using System.Runtime.Serialization;

namespace Alor.OpenAPI.Models
{
    [DataContract]
    public sealed record WsRawWireMessage(
        string? SocketName,
        string? PayloadJson,
        DateTime TimestampUtc,
        DateTime FirstByteTimestampUtc,
        long ReceiveTimestampTicks);

    [DataContract]
    public sealed record WsParseFailure(
        string SocketName,
        string SubscriptionMarker,
        int PayloadLength,
        string ExceptionType,
        string Error,
        DateTime TimestampUtc,
        long ReceiveTimestampTicks);
}

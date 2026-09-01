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
}

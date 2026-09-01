using System.Runtime.Serialization;

namespace Alor.OpenAPI.Models
{
    [DataContract]
    public sealed record CwsRawCommandMessage(
        string? RequestGuid,
        string? PayloadJson,
        DateTime TimestampUtc,
        DateTime SendTimestampUtc,
        long SendTimestampTicks);
}

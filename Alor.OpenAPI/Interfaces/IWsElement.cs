using Alor.OpenAPI.Models;
using System.Collections.Concurrent;

namespace Alor.OpenAPI.Interfaces
{
    public interface IWsElement
    {
        internal DateTime? ReceivedDateTimeUtc { get; set; }
        internal long ReceiveTimestampTicks
        {
            get => 0L;
            set { }
        }
        internal string? Guid { get; }
        internal ConcurrentDictionary<string, Parameters>? Parameters { get; set; }
    }

    internal interface IWsElement<out T> : IWsElement
    {
        T? Data { get; }
    }
}

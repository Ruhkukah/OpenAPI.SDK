namespace Alor.OpenAPI.Interfaces
{
    internal interface ICwsAuthService : IDisposable
    {
        DateTime? AuthorizedUntilUtc { get; }
        string? LastRefreshError { get; }
        Task CwsAuthorizeAndSetRefreshTimer();
    }
}

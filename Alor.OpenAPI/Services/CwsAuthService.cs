using Alor.OpenAPI.Interfaces;
using Alor.OpenAPI.Models;
using Alor.OpenAPI.Utilities;
using Serilog;

namespace Alor.OpenAPI.Services
{
    internal class CwsAuthService : ICwsAuthService
    {
        private static readonly TimeSpan AuthorizedLifetime = TimeSpan.FromMinutes(30);
        private static readonly TimeSpan RefreshInterval = TimeSpan.FromMinutes(10);
        private static readonly TimeSpan MaxRetryInterval = TimeSpan.FromSeconds(30);

        private AlarmClock? _clockAuthorize;
        private readonly ILogger _logger;
        private Func<string, Task<bool>>? _authMsgUpdate;
        private long _messageCount;
        private int _refreshRetryAttempt;

        internal CwsAuthService(ILogger logger, Func<string, Task<bool>> authMsgUpdate)
        {
            _logger = logger;
            _authMsgUpdate = authMsgUpdate;
        }

        public void Dispose()
        {
            _clockAuthorize?.Dispose();
            _authMsgUpdate = null;
        }

        public DateTime? AuthorizedUntilUtc { get; private set; }

        public string? LastRefreshError { get; private set; }

        public async Task CwsAuthorizeAndSetRefreshTimer()
        {
            var nextRefreshDelay = RefreshInterval;
            try
            {
                var message = new CwsRequestAuthorize(
                    Utilities.Utilities.GuidFormatter("auth", Interlocked.Increment(ref _messageCount))).ToJson();
                var sent = await (_authMsgUpdate?.Invoke(message) ?? Task.FromResult(false));
                if (!sent)
                {
                    throw new InvalidOperationException("CWS authorize frame was not sent.");
                }

                AuthorizedUntilUtc = DateTime.UtcNow.Add(AuthorizedLifetime);
                LastRefreshError = null;
                Interlocked.Exchange(ref _refreshRetryAttempt, 0);
            }
            catch (Exception ex)
            {
                LastRefreshError = ex.Message;
                nextRefreshDelay = GetRetryDelay();
                _logger.Error($"Ошибка: {ex.Message}. Повторная CWS авторизация через {nextRefreshDelay.TotalSeconds:F0} с.");
            }

            _clockAuthorize?.Dispose();
            _clockAuthorize = new AlarmClock(DateTime.Now.Add(nextRefreshDelay));
            _clockAuthorize.Alarm += (sender, e) =>
            {
                (sender as AlarmClock)?.Dispose();
                Task.Run(async () =>
                {
                    await CwsAuthorizeAndSetRefreshTimer();
                });
            };
        }

        private TimeSpan GetRetryDelay()
        {
            var attempt = Math.Min(Interlocked.Increment(ref _refreshRetryAttempt), 6);
            var seconds = Math.Min(1 << (attempt - 1), (int)MaxRetryInterval.TotalSeconds);
            return TimeSpan.FromSeconds(seconds);
        }
    }
}

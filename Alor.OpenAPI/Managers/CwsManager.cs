using Alor.OpenAPI.Enums;
using Alor.OpenAPI.Extensions;
using Alor.OpenAPI.Interfaces;
using Alor.OpenAPI.Models;
using System.Collections.Concurrent;

namespace Alor.OpenAPI.Managers
{
    public class CwsManager : ICwsManager
    {
        private long _messageCount;
        private bool _isInitialized;
        private Func<string, Task<(DateTime sendTimestampUtc, long sendTimestampTicks)>>? _commandMsgUpdate;
        private Func<Task>? _cwsAuthorizeAndSetRefreshTimer;
        private Func<DateTime?>? _getAuthorizedUntilUtc;
        private Func<string?>? _getLastAuthorizationError;
        private Action<CwsRawCommandMessage>? _rawCommandMessageHandler;
        private readonly ConcurrentDictionary<string, long> _commandSendTimestampTicksByRequestGuid =
            new(StringComparer.OrdinalIgnoreCase);

        internal CwsManager(
            Func<string, Task<(DateTime sendTimestampUtc, long sendTimestampTicks)>> commandMsgUpdate,
            Func<Task> cwsAuthorizeAndSetRefreshTimer,
            Func<DateTime?>? getAuthorizedUntilUtc = null,
            Func<string?>? getLastAuthorizationError = null)
        {
            _commandMsgUpdate = commandMsgUpdate;
            _cwsAuthorizeAndSetRefreshTimer = cwsAuthorizeAndSetRefreshTimer;
            _getAuthorizedUntilUtc = getAuthorizedUntilUtc;
            _getLastAuthorizationError = getLastAuthorizationError;
        }

        public DateTime? AuthorizedUntilUtc => _getAuthorizedUntilUtc?.Invoke();

        public string? LastAuthorizationError => _getLastAuthorizationError?.Invoke();

        public bool TryGetAndRemoveCommandSendTimestampTicks(string requestGuid, out long sendTimestampTicks) =>
            _commandSendTimestampTicksByRequestGuid.TryRemove(requestGuid, out sendTimestampTicks);

        public void SetRawCommandMessageHandler(Action<CwsRawCommandMessage>? handler) =>
            _rawCommandMessageHandler = handler;

        private async Task EnsureInitialized()
        {
            if (!_isInitialized)
            {
                await (_cwsAuthorizeAndSetRefreshTimer?.Invoke() ?? Task.CompletedTask);
                _isInitialized = true;
            }
        }

        public async Task WarmupAsync()
        {
            await EnsureInitialized();
        }

        public async Task<string> CreateMarketOrderAsync(string portfolio, Side side, int quantity, string symbol,
            Exchange exchange, string? instrumentGroup = null, TimeInForce timeInForce = TimeInForce.OneDay,
            string? comment = null, bool checkDuplicates = true, bool? allowMargin = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderMarket("create:market", guid, null, side, quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, comment, new User(portfolio), timeInForce,
                checkDuplicates, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> CreateLimitOrderAsync(string portfolio, Side side, int quantity, decimal price,
            string symbol, Exchange exchange, string? instrumentGroup = null, string? comment = null,
            TimeInForce timeInForce = TimeInForce.OneDay, int? icebergFixed = null,
            decimal? icebergVariance = null, bool checkDuplicates = true, bool? allowMargin = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderLimit("create:limit", guid, null, side, quantity, price,
                new Instrument(symbol, exchange, instrumentGroup), null, comment, new User(portfolio), timeInForce,
                icebergFixed, icebergVariance, checkDuplicates, allowMargin).ToJson();
            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> CreateStopOrderAsync(string portfolio, Side side,
            Condition condition, decimal triggerPrice, DateTime stopEndUtcTime, int quantity,
            string symbol, Exchange exchange, string? instrumentGroup = null, bool checkDuplicates = true,
            int? protectingSeconds = null, bool? activate = null, bool? allowMargin = null, string? comment = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStop("create:stop", guid, null, side, condition, triggerPrice,
                stopEndUtcTime.GetUnixTimestampSecondsFromDateTime(), quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, new User(portfolio), checkDuplicates, protectingSeconds,
                comment, activate, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> CreateStopLimitOrderAsync(string portfolio, Side side,
            Condition condition, decimal triggerPrice, DateTime stopEndUtcTime, int quantity, decimal price,
            string symbol, Exchange exchange, string? instrumentGroup = null,
            TimeInForce timeInForce = TimeInForce.OneDay, int? icebergFixed = null, decimal? icebergVariance = null,
            bool checkDuplicates = true, int? protectingSeconds = null, bool? activate = null, bool? allowMargin = null,
            string? comment = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStopLimit("create:stopLimit", guid, null, side, condition, triggerPrice,
                stopEndUtcTime.GetUnixTimestampSecondsFromDateTime(), price, quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, new User(portfolio), timeInForce,
                icebergFixed, icebergVariance, checkDuplicates,
                protectingSeconds, comment, activate, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> UpdateMarketOrderAsync(string portfolio, string orderId, Side side, int quantity, string symbol,
            Exchange exchange, string? instrumentGroup = null, TimeInForce timeInForce = TimeInForce.OneDay,
            string? comment = null, bool checkDuplicates = true, bool? allowMargin = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderMarket("update:market", guid, orderId, side, quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, comment, new User(portfolio), timeInForce, checkDuplicates, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> UpdateLimitOrderAsync(string portfolio, string orderId, Side side, int quantity,
            decimal price, string symbol, Exchange exchange, string? instrumentGroup = null, string? comment = null,
            int? icebergFixed = null, bool checkDuplicates = true, bool? allowMargin = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderLimit("update:limit", guid, orderId, side, quantity, price,
                new Instrument(symbol, exchange, instrumentGroup), null, comment, new User(portfolio), null,
                icebergFixed, null, checkDuplicates, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> UpdateStopOrderAsync(string portfolio, string orderId, Side side,
            Condition condition, decimal triggerPrice, DateTime stopEndUtcTime, int quantity,
            string symbol, Exchange exchange, string? instrumentGroup = null, bool checkDuplicates = true,
            int? protectingSeconds = null, bool? activate = null, bool? allowMargin = null,
            string? comment = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStop("update:stop", guid, orderId, side, condition, triggerPrice,
                stopEndUtcTime.GetUnixTimestampSecondsFromDateTime(), quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, new User(portfolio), checkDuplicates, protectingSeconds,
                comment, activate, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> UpdateStopLimitOrderAsync(string portfolio, string orderId, Side side,
            Condition condition, decimal triggerPrice, DateTime stopEndUtcTime, int quantity, decimal price,
            string symbol, Exchange exchange, string? instrumentGroup = null, TimeInForce timeInForce = TimeInForce.OneDay,
            int? icebergFixed = null, bool checkDuplicates = true, int? protectingSeconds = null, bool? activate = null,
            bool? allowMargin = null, string? comment = null)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStopLimit("update:stopLimit", guid, orderId, side, condition, triggerPrice,
                stopEndUtcTime.GetUnixTimestampSecondsFromDateTime(), price, quantity,
                new Instrument(symbol, exchange, instrumentGroup), null, new User(portfolio), timeInForce,
                icebergFixed, null, checkDuplicates,
                protectingSeconds, comment, activate, allowMargin).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> DeleteMarketOrderAsync(string portfolio, string orderId, Exchange exchange, bool checkDuplicates = true)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderMarket("delete:market", guid, orderId, exchange: exchange, user: new User(portfolio), checkDuplicates: checkDuplicates).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> DeleteLimitOrderAsync(string portfolio, string orderId, Exchange exchange, bool checkDuplicates = true)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderLimit("delete:limit", guid, orderId, exchange: exchange, user: new User(portfolio), checkDuplicates: checkDuplicates).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> DeleteStopOrderAsync(string portfolio, string orderId, Exchange exchange, bool checkDuplicates = true)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStop("delete:stop", guid, orderId, exchange: exchange, user: new User(portfolio), checkDuplicates: checkDuplicates).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }

        public async Task<string> DeleteStopLimitOrderAsync(string portfolio, string orderId, Exchange exchange, bool checkDuplicates = true)
        {
            if (string.IsNullOrEmpty(portfolio))
                throw new ArgumentNullException(nameof(portfolio));
            if (string.IsNullOrEmpty(orderId))
                throw new ArgumentNullException(nameof(orderId));

            await EnsureInitialized();

            var guid = Utilities.Utilities.GuidFormatter("a", Interlocked.Increment(ref _messageCount));

            var message = new CwsRequestOrderStopLimit("delete:stopLimit", guid, orderId, exchange: exchange, user: new User(portfolio), checkDuplicates: checkDuplicates).ToJson();

            await SendCommandAsync(guid, message).ConfigureAwait(false);

            return guid;
        }


        public void Dispose()
        {
            _commandMsgUpdate = null;
            _cwsAuthorizeAndSetRefreshTimer = null;
            _getAuthorizedUntilUtc = null;
            _getLastAuthorizationError = null;
            _rawCommandMessageHandler = null;
            _commandSendTimestampTicksByRequestGuid.Clear();
            GC.SuppressFinalize(this);
        }

        private async Task SendCommandAsync(string requestGuid, string message)
        {
            try
            {
                var timestampUtc = DateTime.UtcNow;
                var (sendTimestampUtc, sendTimestampTicks) = _commandMsgUpdate is null
                    ? (timestampUtc, 0L)
                    : await _commandMsgUpdate.Invoke(message).ConfigureAwait(false);
                _commandSendTimestampTicksByRequestGuid[requestGuid] = sendTimestampTicks;
                _rawCommandMessageHandler?.Invoke(
                    new CwsRawCommandMessage(
                        requestGuid,
                        message,
                        timestampUtc,
                        sendTimestampUtc,
                        sendTimestampTicks));
            }
            catch
            {
                _commandSendTimestampTicksByRequestGuid.TryRemove(requestGuid, out _);
                throw;
            }
        }
    }
}

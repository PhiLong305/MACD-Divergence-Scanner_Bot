import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

from twisted.internet import reactor, task, defer
from twisted.internet.defer import inlineCallbacks, DeferredList
import treq
from ctrader_open_api import Client, TcpProtocol, Protobuf
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod
from keep_alive import keep_alive
from divergence import DivergenceState, detect_signals
# ================== CONFIGURATION ==================
TELEGRAM_TOKEN = "8358892572:AAHFNZWXwwd_VIL7veQgdLBjNjI253oLCug"
CHAT_ID = "1676202517"

CTRADER_CLIENT_ID = "16778_at26WTcoFS2NYt1GHQS1gJqaSWorbHZCJKA1X9KRe2z5dZRrMo"
CTRADER_SECRET = "unq1iRL42CtmzTk5MQ9CYdcMfnYmOQSV5Nfu94FEX0ZueystC3"
CTRADER_ACCOUNT_ID = "44322853"
CTRADER_ACCESS_TOKEN = "zVuuTkkAeB_uR0htxndUSRztpBuH6n5h0_2S3K2j6zw"
CTRADER_DEMO_HOST = "demo.ctraderapi.com"
CTRADER_DEMO_PORT = 5035
PAIRS = [
    "XAUUSD", "EURUSD", "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURNZD",
    "GBPUSD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD", "AUDUSD", "AUDCAD",
    "AUDCHF", "AUDNZD", "CADCHF", "USDCHF", "USDCAD", "NZDUSD", "NZDCAD", "NZDCHF"
]
EMA_FAST = 12
EMA_SLOW = 26
MACD_SIGNAL = 9
SCAN_INTERVAL_SEC = 3
MIN_BARS_BACK = 5000
HIST_FETCH_BATCH = 1000
MIN_LOOKBACK_BARS = 10
MAX_LOOKBACK_BARS = 40
SLOPE_THRESHOLD = 0.7
# =============================================
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
class TelegramBot:
    def __init__(self):
        self.offset = 0
    def send(self, text):
        d = treq.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": CHAT_ID, "text": text})
        # Thêm hàm xử lý lỗi Telegram messenge và gắn nó vào Deferred (d)
        def on_error(failure):
            print(f"❌ Lỗi khi gửi tin nhắn Telegram: {failure.getErrorMessage()}")
            return None
        d.addErrback(on_error)
        return d
    def poll(self):
        d = treq.get(f"{TELEGRAM_API}/getUpdates", params={"timeout":0, "offset": self.offset + 1})
        d.addCallback(treq.json_content)
        d.addCallback(self._handle_updates)
        d.addErrback(lambda _: None)
    def _handle_updates(self, data):
        for upd in data.get("result", []):
            self.offset = upd["update_id"]
            msg = upd.get("message", {})
            text = msg.get("text", "").strip()
            if not text:
                continue
            self.handle_command(text)
    def handle_command(self, text):
        if text.startswith("/help"):
            self.send(HELP_TEXT)
        elif text.startswith("/pairs"):
            pairs = "\n".join(f"{i+1}. {p}" for i,p in enumerate(PAIRS))
            self.send(f"📊 Pairs watching:\n{pairs}")
        elif text.startswith("/status"):
            self.send_status()
        elif text.startswith("/stop"):
            stop_scanning()
            self.send("🛑 Scanning stopped")
        elif text.startswith("/scan"):
            parts = text.split(maxsplit=1)
            if len(parts) == 2:
                start_scanning(parts[1].strip())
            else:
                self.send("⚠️ Missing timeframe. Usage: /scan 5m")
        else:
            self.send(f"Echo: {text}")
    def send_status(self):
        if active_timeframes:
            tfs = ", ".join(sorted(active_timeframes))
            self.send(f"✅ Scanning active\nTimeframes: {tfs}\nPairs: {len(PAIRS)}")
        else:
            self.send("⚠️ Bot is idle. Use /scan to start.")
TIMEFRAME_MAP = {
    "5m": ProtoOATrendbarPeriod.M5,
    "15m": ProtoOATrendbarPeriod.M15,
    "30m": ProtoOATrendbarPeriod.M30,
    "1h": ProtoOATrendbarPeriod.H1,
    "4h": ProtoOATrendbarPeriod.H4,
    "1d": ProtoOATrendbarPeriod.D1,
}
PRESETS = {
    "4T": ["5m", "15m", "30m", "1h"],
    "2t": ["5m", "15m"],
    "2T": ["30m", "1h"],
}
market_data = defaultdict(lambda: {
    "closes": deque(maxlen=MIN_BARS_BACK),
    "highs": deque(maxlen=MIN_BARS_BACK),
    "lows": deque(maxlen=MIN_BARS_BACK),
    "hist": deque(maxlen=MIN_BARS_BACK),
    "state": DivergenceState(),
    "last_ts": 0,
})
active_timeframes = set()
class CTraderClient:
    def __init__(self):
        self.client = Client(CTRADER_DEMO_HOST, CTRADER_DEMO_PORT, TcpProtocol)
        self.client.setConnectedCallback(self.on_Connected)
        self.client.setDisconnectedCallback(self.on_Disconnected)
        self.client.setMessageReceivedCallback(self.on_Message)
        self.symbol_ids = {}
        self.pending = []
        self.symbols_loaded_deferred = defer.Deferred() # Thêm Deferred để quản lý việc lấy symbols
        self.subscribed_spots = set() # Lưu các symbol đã subcribed spot
        self.subscribed_trendbars = {} # Dùng để theo dõi đăng ký trendbar
        # Theo dõi những (symbolId, period) đang trong giai đoạn nhận trendbar lịch sử
        self.history_pending = set()
    def start(self):
        """Starts the service"""
        self.client.startService()
    def on_Connected(self, client_instance):
        """Connect to cTrader API"""
        print("✅ Connected to API")
        # NGAY LẬP TỨC gọi chuỗi xác thực sau khi kết nối
        self._run_setup_sequence()
    def on_Disconnected(self, _, reason):
        """Error: Disconnect to cTrader API"""
        print("❌ Disconnected from cTrader API")
        telegram.send("❌ Disconnected from cTrader API")

    @inlineCallbacks
    def _run_setup_sequence(self):
        """
        Chuỗi hành động tuần tự để thiết lập bot:
        1. App Autherize
        2. Acc Autherize
        3. Get symbols
        """
        try:
            # 1. App Autherize
            print("Đang xác thực App...")
            app_auth_req = Protobuf.get('ProtoOAApplicationAuthReq', clientId=CTRADER_CLIENT_ID, clientSecret=CTRADER_SECRET)
            yield self.client.send(app_auth_req)
            print("✅ App auth successful.")

            # 2. Acc Autherize
            acc_auth_req = Protobuf.get('ProtoOAAccountAuthReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), accessToken=CTRADER_ACCESS_TOKEN)
            yield self.client.send(acc_auth_req)
            print("✅ Acc auth successful.")

            # 3. Get symbols
            print("Đang yêu cầu danh sách cặp tiền...")
            symbols_list_req = Protobuf.get('ProtoOASymbolsListReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), includeArchivedSymbols=False)
            yield self.client.send(symbols_list_req)

            # Đợi hàm on_Message xử lý phản hồi từ server
            yield self.symbols_loaded_deferred
            print("✅ Đã tải xong danh sách các cặp tiền.")

        except Exception as e:
            print(f"❌ Lỗi trong quá trình thiết lập: {e}")
            self.client.stopService()
    def on_Message(self, _, message):
        """Hàm callback khi nhận tin nhắn từ server."""
        payload = Protobuf.extract(message)
        ptype = payload.payloadType

        # Print ra lỗi từ API
        if ptype == Protobuf.get_type('ProtoOAErrorRes'):
            print(f"Server Error: Code {payload.errorCode}, Message: {payload.description}")
            return

        # Khi nhận được danh sách symbols, gán vào self.symbol_ids và thông báo Deferred đã hoàn thành
        if ptype == Protobuf.get_type('ProtoOASymbolsListRes'):
            for s in payload.symbol:
                if s.symbolName in PAIRS:
                    self.symbol_ids[s.symbolName] = s.symbolId
            if not self.symbols_loaded_deferred.called:
                 self.symbols_loaded_deferred.callback(None)

        elif ptype == Protobuf.get_type('ProtoOAGetTrendbarsRes'):
            self.handle_trendbars(payload)

    def get_symbol_by_id(self, sid):
        for name, i in self.symbol_ids.items():
            if i == sid:
                return name
        return str(sid)

    def tf_from_period(self, period):
        for k, v in TIMEFRAME_MAP.items():
            if v == period:
                return k
        return str(period)

    @inlineCallbacks
    def subscribe(self, symbol, period):
        yield self.symbols_loaded_deferred

        if symbol not in self.symbol_ids:
            print(f"❌ Cặp tiền {symbol} không tìm thấy.")
            return

        sid = self.symbol_ids[symbol]
        key = (sid, period)
        if key in self.subscribed_trendbars:
            print(f"✅ Đã đăng ký trendbar {symbol}/{self.tf_from_period(period)}.")
            return

        # Đánh dấu cặp (symbolId, period) đang chờ dữ liệu lịch sử
        self.history_pending.add(key)
        self.subscribed_trendbars[key] = 0

        # 1. Đăng ký spot nếu chưa có
        if sid not in self.subscribed_spots:
            try:
                print(f"Đang đăng ký spot cho {symbol}...")
                spot_req = Protobuf.get('ProtoOASubscribeSpotsReq',
                                        ctidTraderAccountId=int(CTRADER_ACCOUNT_ID),
                                        symbolId=[sid])
                yield self.client.send(spot_req)
                self.subscribed_spots.add(sid)
                print(f"✅ Đã đăng ký spot thành công cho {symbol}.")
            except Exception as e:
                print(f"Lỗi đăng ký spot {symbol}: {e}")
                self.history_pending.discard(key)
                self.subscribed_trendbars.pop(key, None)
                return

        # 2. Yêu cầu dữ liệu lịch sử trước
        try:
            print(f"Đang yêu cầu dữ liệu lịch sử cho {symbol}/{self.tf_from_period(period)}...")
            to_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
            hist_req = Protobuf.get(
                'ProtoOAGetTrendbarsReq',
                ctidTraderAccountId=int(CTRADER_ACCOUNT_ID),
                symbolId=sid,
                period=period,
                toTimestamp=to_ts,
                count=min(HIST_FETCH_BATCH, MIN_BARS_BACK),
            )
            yield self.client.send(hist_req)
            print(f"✅ Đã gửi yêu cầu lấy dữ liệu lịch sử cho {symbol}.")
        except Exception as e:
            print(f"Lỗi lấy trendbar lịch sử {symbol}: {e}")
            self.history_pending.discard(key)
            self.subscribed_trendbars.pop(key, None)
            return

        # 3. Đăng ký live trendbar sau khi đã yêu cầu lịch sử
        try:
            print(f"Đang đăng ký live trendbar cho {symbol}...")
            live_trendbar_req = Protobuf.get('ProtoOASubscribeLiveTrendbarReq',
                                             ctidTraderAccountId=int(CTRADER_ACCOUNT_ID),
                                             symbolId=sid, period=period)
            yield self.client.send(live_trendbar_req)
            print(f"✅ Đã đăng ký live trendbar thành công cho {symbol}/{self.tf_from_period(period)}.")
        except Exception as e:
            print(f"Lỗi subscribe trendbar {symbol}: {e}")
            self.history_pending.discard(key)
            self.subscribed_trendbars.pop(key, None)
            return
    @inlineCallbacks
    def unsubscribe(self, symbol, period):
        sid = self.symbol_ids.get(symbol)
        if not sid:
            return

        # Hủy đăng ký live trendbar
        if (sid, period) in self.subscribed_trendbars:
            try:
                req = Protobuf.get('ProtoOAUnsubscribeLiveTrendbarReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), symbolId=sid, period=period)
                yield self.client.send(req)
                del self.subscribed_trendbars[(sid, period)]
                print(f"✅ Đã hủy đăng ký trendbar {symbol}/{self.tf_from_period(period)}")
            except Exception as e:
                print(f"Lỗi khi hủy đăng ký trendbar: {e}")
        # Dọn dẹp trạng thái lịch sử còn chờ (nếu có)
        self.history_pending.discard((sid, period))

    def handle_trendbars(self, payload):
        symbol = self.get_symbol_by_id(payload.symbolId)
        tf = self.tf_from_period(payload.period)
        key = (payload.symbolId, payload.period)
        bars = sorted(payload.trendbar, key=lambda b: b.utcTimestampInMinutes)
        if key in self.history_pending:
            data_loaded = market_data[(symbol, tf)]["closes"]
            iterable = bars if not data_loaded else reversed(bars)
            for tb in iterable:
                process_trendbar(symbol, tf, tb, live=False)
            count_loaded = len(market_data[(symbol, tf)]["closes"])
            if count_loaded < MIN_BARS_BACK:
                print(f"📥 Loaded {count_loaded} bars for {symbol}/{tf} (need {MIN_BARS_BACK})")
                oldest_ts = bars[0].utcTimestampInMinutes * 60 * 1000
                remaining = MIN_BARS_BACK - count_loaded
                hist_req = Protobuf.get(
                    'ProtoOAGetTrendbarsReq',
                    ctidTraderAccountId=int(CTRADER_ACCOUNT_ID),
                    symbolId=payload.symbolId,
                    period=payload.period,
                    toTimestamp=oldest_ts,
                    count=min(HIST_FETCH_BATCH, remaining),
                )
                self.client.send(hist_req)
            else:
                print(f"📥 Loaded {count_loaded} bars for {symbol}/{tf}")
                self.history_pending.discard(key)
                self.subscribed_trendbars[key] = 0
        else:
            count = self.subscribed_trendbars.get(key, 0)
            if count < 1:
                for tb in bars:
                    process_trendbar(symbol, tf, tb, live=False)
                self.subscribed_trendbars[key] = count + 1
            else:
                for tb in bars:
                    process_trendbar(symbol, tf, tb, live=True)

def process_trendbar(symbol, tf, tb, live=True):
    key = (symbol, tf)
    data = market_data[key]
    ts = tb.utcTimestampInMinutes
    if live and ts < data["last_ts"]:
        return
    scale = 1e5
    low = tb.low / scale
    open_ = (tb.low + tb.deltaOpen) / scale
    close = (tb.low + tb.deltaClose) / scale
    high = (tb.low + tb.deltaHigh) / scale
    

    if ts == data["last_ts"] and data["closes"]:
        data["closes"][-1] = close
        data["highs"][-1] = max(data["highs"][-1], high)
        data["lows"][-1] = min(data["lows"][-1], low)
    elif not live and ts < data["last_ts"]:
        data["closes"].appendleft(close)
        data["highs"].appendleft(high)
        data["lows"].appendleft(low)
    else:
        data["closes"].append(close)
        data["highs"].append(high)
        data["lows"].append(low)

    closes = list(data["closes"])
    highs = list(data["highs"])
    lows = list(data["lows"])
    series = pd.Series(closes)
    ema_fast = series.ewm(span=EMA_FAST, adjust=False).mean()
    ema_slow = series.ewm(span=EMA_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal
    data["hist"].clear()
    data["hist"].extend(hist.tolist())
    bar_id = ts if live else None
    signals = detect_signals(
        data["state"], symbol, tf, closes, highs, lows, list(data["hist"]), ts
    )
    if live:
        for s in signals:
            print(s)
            telegram.send(s)
    if ts > data["last_ts"]:
        data["last_ts"] = ts

HELP_TEXT = (
"🤖 MACD Divergence Detection Bot \n"
"📈 Tính năng chính:\n"
"• Phát hiện phân kỳ MACD Classic & Miss Shoulder\n"
"• Theo dõi realtime từ IC Markets\n"
"• Hỗ trợ 22 cặp tiền chính\n"
"• Đa timeframe scanning\n"
"🛠️ Available Commands:\n"
"/help - Hướng dẫn & hỗ trợ\n"
"/status - Kiểm tra trạng thái bot\n"
"/stop - Stop scanning\n"
"/pairs - Danh sách cặp tiền theo dõi\n"
"/scan [timeframe] - Start scanning\n"
"📊 Scan Commands:\n"
"Presets:\n/scan 4T - 5m, 15m, 30m, 1h\n/scan 2t - 5m, 15m\n/scan 2T - 30m, 1h\n"
"Single timeframes:\n/scan 5m, /scan 15m, /scan 30m\n/scan 1h, /scan 4h, /scan 1d\n"
"💡 Tip: Gửi tin nhắn thường để test Echo feature!"
)

telegram = TelegramBot()
ctrader = CTraderClient()
loop = task.LoopingCall(telegram.poll)

@inlineCallbacks
def start_scanning(tf_text):
    """
    Hàm bắt đầu quét, đảm bảo bot đã sẵn sàng trước khi đăng ký
    """
    print("🚀 Đang khởi động quét...")
    # Chờ cho đến khi danh sách symbols được tải xong
    yield ctrader.symbols_loaded_deferred

    # Sau khi chắc chắn đã sẵn sàng, mới bắt đầu đăng ký
    tfs = PRESETS.get(tf_text, [tf_text])
    for tf in tfs:
        if tf not in TIMEFRAME_MAP:
            telegram.send(f"❌ Invalid timeframe {tf}")
            return
    for tf in tfs:
        active_timeframes.add(tf)
        for pair in PAIRS:
            yield ctrader.subscribe(pair, TIMEFRAME_MAP[tf]) # Đảm bảo mỗi lệnh subscribe được gửi đi tuần tự

    telegram.send(
        f"🚀 SCAN STARTED\n⏰ Timeframe: {', '.join(tfs)}\n📈 Pairs: {len(PAIRS)} cặp tiền\n"
        f"🔄 Interval: {SCAN_INTERVAL_SEC} seconds\n📡 Source: IC Markets Demo\n"
        "🎯 Bot sẽ thông báo khi có Divergence Signal!"
    )

def stop_scanning():
    for tf in list(active_timeframes):
        for pair in PAIRS:
            ctrader.unsubscribe(pair, TIMEFRAME_MAP[tf])
    active_timeframes.clear()
    market_data.clear()
    telegram.send("🛑 Scanning stopped")

# KHỞI ĐỘNG BOT
def main_startup_sequence():
    """
    Chuỗi khởi động chính của bot.
    """
    print("🚀 Đang khởi động MACD Divergence Bot...")

    # Gửi tin nhắn chào mừng ngay lập tức
    telegram.send(
        "🤖 MACD Divergence Bot STARTED!\n"
        f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        "📡 Data Source: IC Markets Demo\n"
        f"📈 Monitoring: {len(PAIRS)} currency pairs\n"
        f"🔧 MACD: {EMA_FAST},{EMA_SLOW},{MACD_SIGNAL}\n\n"
        "💡 Type /help for instructions\n"
        "🚀 Ready to scan for divergences!"
    )
    # Khởi động client cTrader.
    ctrader.start()

if __name__ == '__main__':
    keep_alive()
    loop.start(1.0)
    # Chạy chuỗi khởi động chính
    main_startup_sequence()
    # Khởi động reactor để chạy các tác vụ bất đồng bộ
    reactor.run()

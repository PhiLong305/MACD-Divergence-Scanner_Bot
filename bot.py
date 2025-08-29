import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict, deque

from twisted.internet import reactor, task
import treq

from ctrader_open_api import Client, TcpProtocol, Protobuf
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod

from keep_alive import keep_alive
from divergence import DivergenceState, detect_signals

# ================== CONFIGURATION ==================
TELEGRAM_TOKEN = "8358892572:AAHFNZWXwwd_VIL7veQgdLBjNjI253oLCug"
CHAT_ID = "1676202517"

CTRADER_ACCOUNT_ID = "9560393"
CTRADER_CLIENT_ID = "16778_at26WTcoFS2NYt1GHQS1gJqaSWorbHZCJKA1X9KRe2z5dZRrMo"
CTRADER_SECRET = "unq1iRL42CtmzTk5MQ9CYdcMfnYmOQSV5Nfu94FEX0ZueystC3"
CTRADER_ACCESS_TOKEN = "Ztq2opUDDRln67kqczqVC9Giml3xNjmCru46I5BstwU"
CTRADER_DEMO_HOST = "demo.ctraderapi.com"
CTRADER_DEMO_PORT = 5035

PAIRS = [
    "XAUUSD", "EURUSD", "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURNZD",
    "GBPUSD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD", "AUDUSD", "AUDCAD",
    "AUDCHF", "AUDNZD", "CADCHF", "USDCHF", "USDCAD", "NZDUSD", "NZDCAD", "NZDCHF"
]

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

SCAN_INTERVAL_SEC = 3
MIN_BARS_BACK = 5000
MIN_LOOKBACK_BARS = 10
MAX_LOOKBACK_BARS = 40
SLOPE_THRESHOLD = 0.7

# =============================================
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

class TelegramBot:
    def __init__(self):
        self.offset = 0

    def send(self, text):
        return treq.post(f"{TELEGRAM_API}/sendMessage", json={"chat_id": CHAT_ID, "text": text})

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
})

active_timeframes = set()

class CTraderClient:
    def __init__(self):
        self.client = Client(CTRADER_DEMO_HOST, CTRADER_DEMO_PORT, TcpProtocol)
        self.client.setConnectedCallback(self.on_connected)
        self.client.setDisconnectedCallback(self.on_disconnected)
        self.client.setMessageReceivedCallback(self.on_message)
        self.symbol_ids = {}
        self.pending = []

    def start(self):
        self.client.startService()

    def on_connected(self, _):
        auth = Protobuf.get('ProtoOAApplicationAuthReq', clientId=CTRADER_CLIENT_ID, clientSecret=CTRADER_SECRET)
        self.client.send(auth)

    def on_disconnected(self, _, reason):
        telegram.send("❌ Disconnected from IC Markets")

    def on_message(self, _, message):
        payload = Protobuf.extract(message)
        ptype = payload.payloadType
        if ptype == Protobuf.get_type('ProtoOAApplicationAuthRes'):
            acc = Protobuf.get('ProtoOAAccountAuthReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), accessToken=CTRADER_ACCESS_TOKEN)
            self.client.send(acc)
        elif ptype == Protobuf.get_type('ProtoOAAccountAuthRes'):
            req = Protobuf.get('ProtoOASymbolsListReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID))
            self.client.send(req)
        elif ptype == Protobuf.get_type('ProtoOASymbolsListRes'):
            for s in payload.symbol:
                self.symbol_ids[s.symbolName] = s.symbolId
            for sym, per in self.pending:
                self.subscribe(sym, per)
            self.pending.clear()
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

    def subscribe(self, symbol, period):
        if symbol not in self.symbol_ids:
            self.pending.append((symbol, period))
            return
        sid = self.symbol_ids[symbol]
        sub = Protobuf.get('ProtoOASubscribeLiveTrendbarReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), symbolId=sid, period=period)
        self.client.send(sub)
        hist = Protobuf.get('ProtoOAGetTrendbarsReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), symbolId=sid, period=period, count=MIN_BARS_BACK)
        self.client.send(hist)

    def unsubscribe(self, symbol, period):
        if symbol not in self.symbol_ids:
            return
        sid = self.symbol_ids[symbol]
        req = Protobuf.get('ProtoOAUnsubscribeLiveTrendbarReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), symbolId=sid, period=period)
        self.client.send(req)

    def handle_trendbars(self, payload):
        symbol = self.get_symbol_by_id(payload.symbolId)
        tf = self.tf_from_period(payload.period)
        for tb in payload.trendbar:
            process_trendbar(symbol, tf, tb)


def process_trendbar(symbol, tf, tb):
    key = (symbol, tf)
    data = market_data[key]
    scale = 1e5
    low = tb.low / scale
    open_ = (tb.low + tb.deltaOpen) / scale
    close = (tb.low + tb.deltaClose) / scale
    high = (tb.low + tb.deltaHigh) / scale

    data["closes"].append(close)
    data["highs"].append(high)
    data["lows"].append(low)

    if len(data["closes"]) < MACD_SLOW + MACD_SIGNAL:
        return

    closes = list(data["closes"])
    highs = list(data["highs"])
    lows = list(data["lows"])

    series = pd.Series(closes)
    ema_fast = series.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = series.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal

    data["hist"].clear()
    data["hist"].extend(hist.tolist())

    signals = detect_signals(
        data["state"], symbol, tf, closes, highs, lows, list(data["hist"]), tb.utcTimestampInMinutes
    )

    for s in signals:
        telegram.send(s)

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

def start_scanning(tf_text):
    tfs = PRESETS.get(tf_text, [tf_text])
    for tf in tfs:
        if tf not in TIMEFRAME_MAP:
            telegram.send(f"❌ Invalid timeframe {tf}")
            return
    for tf in tfs:
        active_timeframes.add(tf)
        for pair in PAIRS:
            ctrader.subscribe(pair, TIMEFRAME_MAP[tf])
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
if __name__ == '__main__':
    keep_alive()
    loop.start(1.0)
    ctrader.start()
    telegram.send(
        "🤖 MACD Divergence Bot STARTED!\n"
        f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        "📡 Data Source:IC Markets Demo\n"
        f"📈 Monitoring: {len(PAIRS)} currency pairs\n"
        f"🔧 MACD: {MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}\n\n"
        "💡 Type /help for instructions\n"
        "🚀 Ready to scan for divergences!"
    )
    reactor.run()

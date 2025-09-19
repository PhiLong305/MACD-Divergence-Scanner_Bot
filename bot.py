import pandas as pd
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from twisted.internet import reactor, task, defer
from twisted.internet.defer import inlineCallbacks
import treq
import json
import os
from ctrader_open_api import Client, TcpProtocol, Protobuf
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrendbarPeriod
from keep_alive import keep_alive
from divergence import DivergenceState, detect_signals

VIETNAM_timezone = timezone(timedelta(hours=7))  # UTC+7 cho Việt Nam

# ================== CONFIGURATION ==================
TELEGRAM_TOKEN = "8358892572:AAHFNZWXwwd_VIL7veQgdLBjNjI253oLCug"
CHAT_ID = "1676202517"

CTRADER_CLIENT_ID = "16778_at26WTcoFS2NYt1GHQS1gJqaSWorbHZCJKA1X9KRe2z5dZRrMo"
CTRADER_SECRET = "unq1iRL42CtmzTk5MQ9CYdcMfnYmOQSV5Nfu94FEX0ZueystC3"
CTRADER_ACCOUNT_ID = "44322853"
CTRADER_ACCESS_TOKEN = "zVuuTkkAeB_uR0htxndUSRztpBuH6n5h0_2S3K2j6zw"
CTRADER_REFRESH_TOKEN = "vad60eaFw4bMvn8OrPEAuizInEJjK_CHwMc3QJO3LOI"
CTRADER_DEMO_HOST = "demo.ctraderapi.com"
CTRADER_DEMO_PORT = 5035

# File lưu token info (Application Spotware)
TOKEN_FILE = "token_info.json"    # File lưu thông tin token
TOKEN_REFRESH_MARGIN_MINUTES = 5  # Refresh token 5 phút trước khi hết hạn

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
MIN_LOOKBACK_BARS = 10
MAX_LOOKBACK_BARS = 40
SLOPE_THRESHOLD = 0.7

TIMEFRAME_MAP = {
    "5m": ProtoOATrendbarPeriod.M5,
    "15m": ProtoOATrendbarPeriod.M15,
    "30m": ProtoOATrendbarPeriod.M30,
    "1h": ProtoOATrendbarPeriod.H1,
    "4h": ProtoOATrendbarPeriod.H4,
    "1d": ProtoOATrendbarPeriod.D1,
}
PERIOD_SECONDS = {
    ProtoOATrendbarPeriod.M5: 300,
    ProtoOATrendbarPeriod.M15: 900,
    ProtoOATrendbarPeriod.M30: 1800,
    ProtoOATrendbarPeriod.H1: 3600,
    ProtoOATrendbarPeriod.H4: 14400,
    ProtoOATrendbarPeriod.D1: 86400,
    ProtoOATrendbarPeriod.W1: 604800,
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
    "bar_count": 0,          # Đếm tổng số bar nhận được
    "history(-1)_bars": 0,   # Đếm bar lịch sử (-1)
    "last_history_bars": 0,   # Đếm bar lịch sử gần nhất
    "first_bar_time": None,  # Thời gian bar đầu tiên
    "last_bar_time": None,   # Thời gian bar cuối cùng
})
active_timeframes = set()

# THÊM HÀM KIỂM TRA COMPLETED BARS
def check_completed_bar_delays():
    """Kiểm tra xem có key nào không nhận được completed bar mới nhất"""
    current_time = datetime.now(VIETNAM_timezone)
    current_minutes = int(current_time.timestamp() / 60)

    print(f"\n🔍 Checking completed bars at {current_time.strftime('%H:%M:%S')}")

    missing_bars = []

    for key, data in market_data.items():
        if isinstance(key, tuple) and len(key) == 2:
            symbol, tf = key
            last_bar_time = data.get("last_bar_time", 0)

            if last_bar_time == 0:
                continue

            # Lấy period của timeframe
            period_seconds = PERIOD_SECONDS.get(TIMEFRAME_MAP.get(tf), 300)
            period_minutes = period_seconds // 60

            # Tính toán completed bar mới nhất mà bot SHOULD có
            # Ví dụ: 17:07, period 5m -> latest completed should be 17:05
            expected_latest = (current_minutes // period_minutes) * period_minutes

            # Nếu bot's last bar cũ hơn expected latest completed bar
            if last_bar_time < expected_latest:
                minutes_missing = expected_latest - last_bar_time

                last_bar_str = datetime.fromtimestamp(last_bar_time * 60, VIETNAM_timezone).strftime('%H:%M')
                expected_str = datetime.fromtimestamp(expected_latest * 60, VIETNAM_timezone).strftime('%H:%M')

                missing_bars.append({
                    'key': f"{symbol}/{tf}",
                    'last_received': last_bar_str,
                    'should_have': expected_str,
                    'missing_minutes': minutes_missing
                })

                print(f"❌ {symbol}/{tf}: Last={last_bar_str}, Should have={expected_str} ({minutes_missing}m missing)")

    if not missing_bars:
        print("✅ All pairs have update latest completed bars")

    return missing_bars

# ================== BAR COUNTER SYSTEM ==================
bar_stats = defaultdict(lambda: {
    "total_bars": 0,
    "history(-1)_bars": 0,
    "last_history_bars": 0,
    "first_received": None,
    "last_received": None,
    "data_quality": "Unknown"
})

def print_bar_stats():
    """In thống kê số lượng bar nhận được theo milestone"""
    if not bar_stats:
        print("📊 Chưa có dữ liệu bar nào")
        return

    print("\n" + "="*60)
    print("📊 BAR STATISTICS REPORT")
    print("="*60)

    for key, stats in bar_stats.items():
        symbol, tf = key
        total = stats['total_bars']
        progress = f"{total}/{MIN_BARS_BACK}"
        percentage = (total / MIN_BARS_BACK * 100) if total > 0 else 0

        print(f"🔹 {symbol}/{tf}: {progress} ({percentage:.1f}%)")
        print(f"   📜 History(-1): {stats['history(-1)_bars']} | 🔴 Newest_history: {stats['last_history_bars']}")

        # Đánh giá chất lượng dựa trên tỷ lệ hoàn thành
        if percentage >= 80:
            quality = "✅ Excellent"
        elif percentage >= 50:
            quality = "🟡 Good"  
        elif percentage >= 20:
            quality = "🟠 Fair"
        else:
            quality = "🔴 Poor"

        print(f"   📋 Quality: {quality}")

    print("="*60)

def get_bar_summary():
    """Tạo summary ngắn gọn về bar stats"""
    if not bar_stats:
        return "📊 No bar data available"

    total_symbols = len(bar_stats)
    total_bars = sum(stats['total_bars'] for stats in bar_stats.values())
    total_live = sum(stats['last_history_bars'] for stats in bar_stats.values())

    return f"📊 {total_symbols} symbols | {total_bars} total bars | {total_live} live bars"

def reset_bar_stats():
    """Reset tất cả thống kê bar"""
    bar_stats.clear()
    for key in market_data.keys():
        market_data[key]["bar_count"] = 0
        market_data[key]["history(-1)_bars"] = 0
        market_data[key]["last_history_bars"] = 0
        market_data[key]["first_bar_time"] = None
        market_data[key]["last_bar_time"] = None
    print("🔄 Đã reset tất cả bar statistics")

class TokenManager:
    """Quản lý Access Token và Refresh Token"""

    def __init__(self):
        self.access_token = CTRADER_ACCESS_TOKEN
        self.refresh_token = CTRADER_REFRESH_TOKEN
        self.expires_at = None
        self.refresh_task = None
        self.load_token_info()

    def load_token_info(self):
        """Tải thông tin token từ file"""
        try:
            if os.path.exists(TOKEN_FILE):
                with open(TOKEN_FILE, 'r') as f:
                    data = json.load(f)
                    self.access_token = data.get('access_token', CTRADER_ACCESS_TOKEN)
                    self.refresh_token = data.get('refresh_token', CTRADER_REFRESH_TOKEN)
                    expires_str = data.get('expires_at')
                    if expires_str:
                        self.expires_at = datetime.fromisoformat(expires_str.replace('Z', '+00:00'))
                    print(f"✅ Đã tải token info từ {TOKEN_FILE}")
        except Exception as e:
            print(f"⚠️ Không thể tải token info: {e}")

    def save_token_info(self):
        """Lưu thông tin token vào file"""
        try:
            data = {
                'access_token': self.access_token,
                'refresh_token': self.refresh_token,
                'expires_at': self.expires_at.isoformat() if self.expires_at else None,
                'updated_at': datetime.now(VIETNAM_timezone).isoformat()
            }
            with open(TOKEN_FILE, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"✅ Đã lưu token info vào {TOKEN_FILE}")
        except Exception as e:
            print(f"❌ Lỗi lưu token info: {e}")

    def update_tokens(self, access_token, refresh_token=None, expires_in_seconds=None):
        """Cập nhật token info"""
        self.access_token = access_token
        if refresh_token:
            self.refresh_token = refresh_token

        if expires_in_seconds:
            self.expires_at = datetime.now(VIETNAM_timezone) + timedelta(seconds=expires_in_seconds)
        elif not self.expires_at:
            # Mặc định access token có hạn 30 ngày
            self.expires_at = datetime.now(VIETNAM_timezone) + timedelta(days=30)

        self.save_token_info()
        self.schedule_refresh()

    def should_refresh_token(self):
        """Kiểm tra có cần refresh token không"""
        if not self.expires_at:
            return False

        time_until_expiry = self.expires_at - datetime.now(VIETNAM_timezone)
        return time_until_expiry.total_seconds() <= (TOKEN_REFRESH_MARGIN_MINUTES * 60)

    def schedule_refresh(self):
        """Lên lịch refresh token"""
        if self.refresh_task and self.refresh_task.active():
            self.refresh_task.cancel()

        if not self.expires_at:
            return

        time_until_refresh = self.expires_at - datetime.now(VIETNAM_timezone) - timedelta(minutes=TOKEN_REFRESH_MARGIN_MINUTES)

        if time_until_refresh.total_seconds() > 0:
            self.refresh_task = reactor.callLater(
                time_until_refresh.total_seconds(),
                lambda: ctrader.refresh_access_token()
            )
            print(f"📅 Đã lên lịch refresh token lúc: {(datetime.now(VIETNAM_timezone) + time_until_refresh).strftime('%Y-%m-%d %H:%M:%S UTC+7')}")
        else:
            # Token sắp hết hạn, refresh ngay
            reactor.callLater(0, lambda: ctrader.refresh_access_token())

class CTraderClient:
    def __init__(self):
        self.client = Client(CTRADER_DEMO_HOST, CTRADER_DEMO_PORT, TcpProtocol)
        self.client.setConnectedCallback(self.on_Connected)
        self.client.setDisconnectedCallback(self.on_Disconnected)
        self.client.setMessageReceivedCallback(self.on_Message)
        self.symbol_ids = {}
        self.pending = []
        self.symbols_loaded_deferred = defer.Deferred() # Thêm Deferred để quản lý việc lấy symbols
        self.subscribed_trendbars = {} # Dùng để theo dõi đăng ký trendbar
        self.history_pending = set()   # Set lưu trữ các key đang trong trạng thái chờ nhận dữ liệu nến lịch sử.
        self.token_manager = TokenManager()
        self.refresh_in_progress = False

    def start(self):
        """Starts the service"""
        self.client.startService()
    def on_Connected(self, client_instance):
        print("✅ Connected to API")
        # Check token trước khi bắt đầu gọi chuỗi xác thực sau Connected
        if self.token_manager.should_refresh_token() and self.token_manager.refresh_token:
            print("🔄 Token sắp hết hạn, thực hiện refresh...")
            self.refresh_access_token()
        else:
            self._run_setup_sequence()
    def on_Disconnected(self, _, reason):
        print("❌ Disconnected from cTrader API")
        telegram.send("❌ Disconnected from cTrader API")
        reactor.stop()

    @inlineCallbacks
    def refresh_access_token(self):
        """Refresh access token using refresh token"""
        if self.refresh_in_progress:
            print("🔄 Refresh đang được thực hiện...")
            return

        if not self.token_manager.refresh_token:
            print("❌ Không có refresh token để thực hiện refresh")
            telegram.send("❌ Không có refresh token - cần cập nhật thủ công")
            return

        try:
            self.refresh_in_progress = True
            print("🔄 Đang refresh access token...")

            refresh_req = Protobuf.get('ProtoOARefreshTokenReq', 
                                     refreshToken=self.token_manager.refresh_token)
            yield self.client.send(refresh_req)

        except Exception as e:
            print(f"❌ Lỗi khi refresh token: {e}")
            telegram.send(f"❌ Lỗi refresh token: {e}")
            self.refresh_in_progress = False

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
            acc_auth_req = Protobuf.get('ProtoOAAccountAuthReq', ctidTraderAccountId=int(CTRADER_ACCOUNT_ID), accessToken=self.token_manager.access_token)
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
            print(f"❌ Lỗi trong quá trình thiết lập ban đầu: {e}")
            self.client.stopService()
            reactor.stop()

    def on_Message(self, _, message):
        """Hàm callback khi nhận tin nhắn từ server."""

        # Giải mã decode thô từ server trả về
        payload = Protobuf.extract(message)
        ptype = payload.payloadType

        # Nếu là dạng tin nhắn Error -> Print ra lỗi từ API
        if ptype == Protobuf.get_type('ProtoOAErrorRes'):
            print(f"Server Error: Code {payload.errorCode}, Message: {payload.description}")
            # Nếu lỗi liên quan đến token
            if payload.errorCode in ["INVALID_TOKEN", "TOKEN_EXPIRED"]:
                telegram.send("❌ Token hết hạn hoặc không hợp lệ!")
            return
        # Xử lý phản hồi refresh token
        elif ptype == Protobuf.get_type('ProtoOARefreshTokenRes'):
            self.handle_refresh_token_response(payload)
            return

        # Nếu là dạng SymbolListRes, Khi received danh sách symbols, gán vào self.symbol_ids và thông báo Deferred đã hoàn thành
        if ptype == Protobuf.get_type('ProtoOASymbolsListRes'):
            for s in payload.symbol:
                if s.symbolName in PAIRS:
                    self.symbol_ids[s.symbolName] = s.symbolId
            if not self.symbols_loaded_deferred.called:
                 self.symbols_loaded_deferred.callback(None)

        # Nếu là type TrendbarsRes, gọi hàm xử lý 
        elif ptype == Protobuf.get_type('ProtoOAGetTrendbarsRes'):
            self.handle_trendbars(payload)

    def handle_refresh_token_response(self, payload):
        """Xử lý phản hồi refresh token"""
        try:
            # Cập nhật token mới
            self.token_manager.update_tokens(
                access_token=payload.accessToken,
                refresh_token=getattr(payload, 'refreshToken', None),
                expires_in_seconds=getattr(payload, 'expiresIn', None)
            )

            print("✅ Refresh token thành công!")
            telegram.send("🔄 Access token đã được refresh thành công!")

            # Tiếp tục với setup nếu chưa hoàn thành
            if not hasattr(self, 'symbols_loaded_deferred') or not self.symbols_loaded_deferred.called:
                self._run_setup_sequence()

        except Exception as e:
            print(f"❌ Lỗi xử lý refresh token response: {e}")
            telegram.send(f"❌ Lỗi xử lý refresh token: {e}")
        finally:
            self.refresh_in_progress = False

    def get_symbol_name_by_id(self, sid):
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
        yield self.symbols_loaded_deferred # Chờ cho đến khi received Symbol List

        if symbol not in self.symbol_ids:
            print(f"❌ Cặp tiền {symbol} không tìm thấy trong PAIRS.")
            return

        sid = self.symbol_ids[symbol]
        key = (sid, period)
        tf = self.tf_from_period(period)

        if key in self.subscribed_trendbars:
            print(f"✅ Đã đăng ký trendbar {symbol}/{tf}.")
            return

        # Đánh dấu (symbol,tf) đang chờ dữ liệu lịch sử và kiểm tra trạng thái subcribe
        self.history_pending.add(key)      # Add key vào set hàng chờ nhận dữ liệu lịch sử
        self.subscribed_trendbars[key] = 0

        # 1. Yêu cầu dữ liệu lịch sử
        try:
            print(f"Đang yêu cầu dữ liệu lịch sử cho {symbol}/{self.tf_from_period(period)}...")
            time_now = datetime.now(VIETNAM_timezone)
            period_seconds = PERIOD_SECONDS[period]
            from_ts = int((time_now - timedelta(seconds=period_seconds * MIN_BARS_BACK)).timestamp() * 1000)
            to_ts = int(time_now.timestamp() * 1000)

            hist_req = Protobuf.get('ProtoOAGetTrendbarsReq',
                                    ctidTraderAccountId=int(CTRADER_ACCOUNT_ID),
                                    symbolId=sid,
                                    period=period,
                                    fromTimestamp=from_ts,
                                    toTimestamp=to_ts,
                                    count=MIN_BARS_BACK)
            yield self.client.send(hist_req)
            print(f"✅ Đã gửi yêu cầu lấy dữ liệu lịch sử cho {symbol}.")
        except Exception as e:
            print(f"Lỗi lấy trendbar lịch sử {symbol}: {e}")
            self.history_pending.discard(key)
            self.subscribed_trendbars.pop(key, None)
            return

    @inlineCallbacks
    def unsubscribe(self, symbol, period):
        sid = self.symbol_ids.get(symbol)
        if not sid:
            return

        # Dọn dẹp trạng thái lịch sử còn chờ (nếu có)
        self.history_pending.discard((sid, period))

    def handle_trendbars(self, payload):
        symbol = self.get_symbol_name_by_id(payload.symbolId)
        tf = self.tf_from_period(payload.period)
        key = (payload.symbolId, payload.period)

        # Đếm số lượng bar trong payload
        bar_count = len(payload.trendbar)

        if key in self.history_pending:
            print(f"📜 {symbol}/{tf}: Nhận {bar_count} history bars")

            # Build state bằng toàn bộ history trừ nến cuối
            for tb in payload.trendbar[:-1]:
                process_trendbar(symbol, tf, tb, live=False)

            # 👉 Detect signal trên nến lịch sử gần hiện tại nhất
            if payload.trendbar:
                last_tb = payload.trendbar[-1]
                process_trendbar(symbol, tf, last_tb, live=True)

            # Cập nhật lại state
            self.history_pending.discard(key)
            self.subscribed_trendbars[key] = 1
            return

        # Nếu lần đầu subscribe -> coi như initial batch
        if key not in self.subscribed_trendbars:
            print(f"📊 {symbol}/{tf}: Nhận {bar_count} initial bars")
            for tb in payload.trendbar[:-1]:
                process_trendbar(symbol, tf, tb, live=False)

            last_tb = payload.trendbar[-1]
            process_trendbar(symbol, tf, last_tb, live=True)

            self.subscribed_trendbars[key] = 1
            return

        # Ngược lại -> đây là live bars (bar lịch sử gần nhất)
        print(f"🔴 {symbol}/{tf}: Nhận {bar_count} LIVE bars")
        for tb in payload.trendbar[:-1]:
            process_trendbar(symbol, tf, tb, live=False)

        last_tb = payload.trendbar[-1]
        process_trendbar(symbol, tf, last_tb, live=True)

def process_trendbar(symbol, tf, tb, live=True):
    key = (symbol, tf)
    data = market_data[key]
    if tb.utcTimestampInMinutes <= data["last_ts"]:
        return

    # Cập nhật counter
    data["bar_count"] += 1
    if live:
        data["last_history_bars"] += 1
        bar_stats[key]["last_history_bars"] += 1
    else:
        data["history(-1)_bars"] += 1  
        bar_stats[key]["history(-1)_bars"] += 1

    bar_stats[key]["total_bars"] += 1

    # Cập nhật thời gian
    if not data["first_bar_time"]:
        data["first_bar_time"] = tb.utcTimestampInMinutes
        bar_stats[key]["first_received"] = tb.utcTimestampInMinutes

    # Process price data
    scale = 1e5
    low = tb.low / scale
    close = (tb.low + tb.deltaClose) / scale
    high = (tb.low + tb.deltaHigh) / scale

    data["closes"].append(close) # Trượt set History để +update trendbar, -oldest_trendbar
    data["highs"].append(high)
    data["lows"].append(low)

    # Calculate indicators
    closes = list(data["closes"])
    highs = list(data["highs"])
    lows = list(data["lows"])
    series = pd.Series(closes)
    ema_fast = series.ewm(span=EMA_FAST, adjust=False).mean()
    ema_slow = series.ewm(span=EMA_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    hist = macd_line - signal
    
    # ✨ CHỈ LẤY GIÁ TRỊ HIST CUỐI CÙNG - KHÔNG GHI ĐÈ HẾT!
    new_hist_value = hist.iloc[-1]       # Chỉ lấy giá trị hist mới nhất
    data["hist"].append(new_hist_value)  # Chỉ thêm 1 giá trị hist

    # Detect Signals
    signals = detect_signals(
        data["state"], symbol, tf, closes, highs, lows, list(data["hist"]), tb.utcTimestampInMinutes
    )  
    if live:
        for s in signals:
            telegram.send(s)
    data["last_ts"] = tb.utcTimestampInMinutes
    data["last_bar_time"] = tb.utcTimestampInMinutes  # THÊM DÒNG NÀY

    # Print package milestone cho history bars (every package:1000 bars)
    total_bars = data["bar_count"]
    if not live and total_bars % 1000 == 0:
        percentage = (total_bars / MIN_BARS_BACK * 100) if total_bars > 0 else 0
        print(f"📊 {symbol}/{tf}: {total_bars}/{MIN_BARS_BACK} ({percentage:.1f}%) - History milestone")

    # Print periodic stats cho live bars (bar lịch sử gần nhất) (every:10 bars)  
    if live and data["last_history_bars"] % 10 == 0:
        bar_time = datetime.fromtimestamp(tb.utcTimestampInMinutes * 60, VIETNAM_timezone)
        print(f"🔴 {symbol}/{tf}: {data['last_history_bars']} New_history bars | Latest: {bar_time.strftime('%H:%M')}")

# TELEGRAM BOT =============================================
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
        elif text.startswith("/token status"):
            self.send_token_status()
        elif text.startswith("/refresh"):
            ctrader.refresh_access_token()
        elif text.startswith("/bars"):
            self.send_bar_stats()
        elif text.startswith("/reset_bars"):
            reset_bar_stats()
            self.send("🔄 Bar statistics đã được reset")
        elif text.startswith("/print_bars"):
            print_bar_stats()  # Print ra console
            self.send("📊 Bar statistics đã được in ra console")
        elif text.startswith("/check"):
            missing = check_completed_bar_delays()
            if missing:
                msg = f"❌ {len(missing)} pairs missing completed bars:\n"
                for bar in missing[:8]:
                    msg += f"🔸 {bar['key']}: missing {bar['missing_minutes']}m\n"
                    msg += f"   Last: {bar['last_received']} | Should: {bar['should_have']}\n"
            else:
                msg = "✅ All pairs have latest completed bars"
            self.send(msg)
        elif text.startswith("/stop"):
            stop_scanning()
            self.send("🛑 Scanning stopped")
        elif text.startswith("/scan"):
            parts = text.split(maxsplit=1)
            if len(parts) == 2:
                start_scanning(parts[1].strip())
            else:
                self.send("⚠️ Missing timeframe. Usage: /scan {timeframe}")
        else:
            self.send(f"Echo: {text}")

    def send_status(self):
        if active_timeframes:
            tfs = ", ".join(sorted(active_timeframes))
            self.send(f"✅ Scanning active\nTimeframes: {tfs}\nPairs: {len(PAIRS)}")
        else:
            self.send("⚠️ Bot is idle. Use /scan to start.")

    def send_token_status(self):
        """Gửi thông tin trạng thái token"""
        tm = ctrader.token_manager
        if tm.expires_at:
            time_left = tm.expires_at - datetime.now(VIETNAM_timezone)
            days_left = time_left.days
            hours_left = time_left.seconds // 3600

            status = f"🔑 TOKEN STATUS\n"
            status += f"⏰ Expires: {tm.expires_at.strftime('%Y-%m-%d %H:%M UTC')}\n"
            status += f"⏳ Time left: {days_left}d {hours_left}h\n"
            status += f"🔄 Refresh token: {'✅' if tm.refresh_token else '❌'}\n"
            status += f"📅 Auto refresh: {'✅' if tm.refresh_task and tm.refresh_task.active() else '❌'}"
        else:
            status = "⚠️ Không có thông tin thời hạn token"

        self.send(status)

    def send_bar_stats(self):
        """Gửi thống kê bar qua Telegram - Tổng kết ngắn gọn"""
        if not bar_stats:
            self.send("📊 Chưa có dữ liệu bar nào")
            return

        message = f"📊 BAR SUMMARY ({len(bar_stats)} symbols)\n"
        message += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"

        # Sắp xếp theo tên symbol để dễ đọc
        sorted_stats = sorted(bar_stats.items(), key=lambda x: (x[0][0], x[0][1]))

        for (symbol, tf), stats in sorted_stats:
            total = stats['total_bars']
            progress = f"{total}/{MIN_BARS_BACK}"
            percentage = (total / MIN_BARS_BACK * 100) if total > 0 else 0

            # Icon dựa trên tỷ lệ hoàn thành
            if percentage >= 80:
                icon = "✅"
            elif percentage >= 50:
                icon = "🟡"
            elif percentage >= 20:
                icon = "🟠"
            else:
                icon = "🔴"

            message += f"{icon} {symbol}/{tf}: {progress}\n"

        message += "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        message += "💡 /print_bars for detailed console view"

        self.send(message)

HELP_TEXT = (
"🤖 MACD Divergence Detection Bot \n"
"📈 Tính năng chính:\n"
"• Phát hiện phân kỳ MACD Classic & Miss Shoulder\n"
"• Theo dõi realtime từ IC Markets\n"
"• Hỗ trợ 22 cặp tiền chính\n"
"• Đa timeframe scanning\n"
"• Auto refresh token\n"
"• Bar counting & monitoring\n"
"🛠️ Available Commands:\n"
"/help - Hướng dẫn & hỗ trợ\n"
"/status - Kiểm tra trạng thái bot\n"
"/token - Kiểm tra trạng thái token\n"
"/refresh - Thủ công refresh token\n"
"/bars - Xem thống kê bar data\n"
"/print_bars - In chi tiết bar stats (console)\n"
"/reset_bars - Reset bar statistics\n"
"/missing - Kiểm tra pairs thiếu update 🆕\n"
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
    Hàm bắt đầu Scanning, đảm bảo bot đã sẵn sàng trước khi đăng ký
    """
    print("🚀 Đang khởi động Scanning...")
    # Chờ ở đây cho đến khi danh sách symbols được tải xong
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
    print("\n🛑 FINAL BAR STATISTICS BEFORE STOP:")
    print_bar_stats()

# KHỞI ĐỘNG BOT
def main_startup_sequence():
    """
    Chuỗi khởi động chính của bot.
    """
    print("🚀 Đang khởi động MACD Divergence Bot...")

    # Gửi tin nhắn chào mừng ngay lập tức
    telegram.send(
        "🤖 MACD Divergence Bot STARTED!\n"
        f"⏰ Time: {datetime.now(VIETNAM_timezone).strftime('%Y-%m-%d %H:%M:%S UTC+7')}\n"
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

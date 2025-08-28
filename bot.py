# -*- coding: utf-8 -*-
import asyncio
import math
import logging
import time
import json
import aiohttp # 1 thư viện giúp bot giao tiếp với các dịch vụ web khác (như API của cTrader)
from datetime import datetime, timezone
from collections import defaultdict, deque
from keep_alive import keep_alive

from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
import ctrader_open_api.messages.OpenApiMessages_pb2 as OA
import ctrader_open_api.messages.OpenApiModelMessages_pb2 as OAModel
import ctrader_open_api.messages.OpenApiCommonMessages_pb2 as OACommon
import ctrader_open_api.messages.OpenApiCommonModelMessages_pb2 as OAModelCommon
from twisted.internet import reactor, defer
from twisted.internet.task import LoopingCall

keep_alive()  # Khởi động web server
# --------------------------------
    
# ================== CONFIGURATION ==================
TELEGRAM_TOKEN = "8358892572:AAHFNZWXwwd_VIL7veQgdLBjNjI253oLCug"
CHAT_ID = "1676202517"

# Tài khoản cTrader demo (để feed giá từ ICMarkets)
CTRADER_PASSWORD   = "philong00"
CTRADER_SERVER     = "IC Markets Global cTrader Demo"

# List pairs to Scan
PAIRS = [
    "XAUUSD", "EURUSD", "EURAUD", "EURCAD", "EURCHF", "EURGBP", "EURNZD",
    "GBPUSD", "GBPAUD", "GBPCAD", "GBPCHF", "GBPNZD", "AUDUSD", "AUDCAD",
    "AUDCHF", "AUDNZD", "CADCHF", "USDCHF", "USDCAD", "NZDUSD", "NZDCAD", "NZDCHF"
]

# Updated timeframe presets
PRESETS = {
    "4T": ["5m", "15m", "30m", "1h"],
    "2t": ["5m", "15m"],
    "2T": ["30m", "1h"],
    "m5": ["5m"],
    "m15": ["15m"],
    "m30": ["30m"],
    "1h": ["1h"],
    "4h": ["4h"],
    "1d": ["1d"]
}

# MACD Settings
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Divergence Settings
SCAN_INTERVAL_SEC = 3
MIN_BARS_BACK = 5000
MIN_LOOKBACK_BARS = 10
MAX_LOOKBACK_BARS = 40
SLOPE_THRESHOLD = 0.7

# Cấu hình log thông báo, save history và theo dõi status bot trong file macd_bot.log trên Replit của App bot này
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("macd_bot.log")
    ]
)

# ================== UTILITIES ==================
# ---- Chuyển timeframe từ text -> phút ----
def tf_to_minutes(tf: str) -> int:
    t = tf.lower() # Chuyển đổi thành chữ thường, không in hoa để xử lý dễ hơn
    if t.endswith("m"): return int(t[:-1]) #Pine Script, [0] = hiện tại; Python là [-1] 
    if t.endswith("h"): return int(t[:-1]) * 60 # h=m*60
    if t.endswith("d"): return int(t[:-1]) * 60 * 24 # d=m*60*24
    raise ValueError(f"Invalid timeframe: {tf}")

# EMA (Logic Công thức tính)
def ema(series, period):
    if period <= 0 or len(series) == 0: return []
    k = 2 / (period + 1)
    out = [series[0]]
    prev = series[0]
    for i in range(1, len(series)):
        prev = series[i] * k + prev * (1 - k)
        out.append(prev)
    return out

# MACD-Histogram (Logic Công thức tính)
def macd_hist(closes, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
    if len(closes) < slow + signal + 5: return []
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)] #Không thể làm kiểu: macd_line = ema_fast - ema_slow, sẽ lỗi, vì  ema_fasf và slow là mảng, không cố định value
    signal_line = ema(macd_line, signal)
    return [m - s for m, s in zip(macd_line, signal_line)]

# ================== DIVERGENCE DETECTION (Pine Script -> Python) =========
class ClassicDivergenceState:
    def __init__(self): # Hàm init: Khởi tạo các giá trị ban đầu, khai báo các biến
        # Classic Bearish
        self.Classic_bearZoneA_high_highest = math.nan
        self.Classic_bearZoneA_hist_highest = math.nan
        self.Classic_in_bearZoneB = False
        self.Classic_bearZoneB_occurred = False
        # Calculate in Track sequences >= 0
        self.Classic_max_high_in_sequence = math.nan
        self.Classic_max_hist_in_sequence = math.nan
        self.Classic_bearZoneA_bar_highest = math.nan
        self.Classic_bearZoneA_hist_highest_barIndex = math.nan

        # Classic Bullish
        self.Classic_bullZoneA_low_lowest = math.nan
        self.Classic_bullZoneA_hist_lowest = math.nan
        self.Classic_in_bullZoneB = False
        self.Classic_bullZoneB_occurred = False
        # Calculate in Track sequence < 0
        self.Classic_min_low_in_sequence = math.nan
        self.Classic_min_hist_in_sequence = math.nan
        self.Classic_bullZoneA_bar_lowest = math.nan
        self.Classic_bullZoneA_hist_lowest_barIndex = math.nan

class MissShoulderDivergenceState:
    def __init__(self):
        # Miss Shoulder Bearish
        self.MS_bearZoneA_high_highest = math.nan
        self.MS_bearZoneA_hist_highest = math.nan
        self.MS_in_bearZoneB = False
        self.MS_bearZoneB_occurred = False
        # Calculate in Track sequences >= 0
        self.MS_max_high_in_sequence = math.nan
        self.MS_max_hist_in_sequence = math.nan
        self.MS_bearZoneA_bar_highest = math.nan
        self.MS_bearZoneA_hist_highest_barIndex = math.nan

        # Miss Shoulder Bullish
        self.MS_bullZoneA_low_lowest = math.nan
        self.MS_bullZoneA_hist_lowest = math.nan
        self.MS_in_bullZoneB = False
        self.MS_bullZoneB_occurred = False
        # Calculate in Track sequences < 0
        self.MS_min_low_in_sequence = math.nan
        self.MS_min_hist_in_sequence = math.nan
        self.MS_bullZoneA_bar_lowest = math.nan
        self.MS_bullZoneA_hist_lowest_barIndex = math.nan

# class Anti-spam Signal (Classic & Miss Shoulder)
class DivergenceState:
    def __init__(self):
        self.classic = ClassicDivergenceState()
        self.miss_shoulder = MissShoulderDivergenceState()
        # Anti-spam
        self.last_bar_id_bearish_classic = None
        self.last_bar_id_bullish_classic = None
        self.last_bar_id_bearish_ms = None
        self.last_bar_id_bullish_ms = None

# ================== DETECTION ClASSIC DIVERGENCE LOGIC ==================
def detect_classic_divergence(state: ClassicDivergenceState, closes, highs, lows, hist, bar_index, min_lookbackBars, max_lookbackBars, slopeThreshold):
    signals = {"bearish": False, "bullish": False}
    if len(hist) < 2: # Mặc định chưa phát hiện phân kỳ nào
        return signals # Nếu dữ liệu hist quá ít -> Trả signal về False

    h0 = hist[-1]  # current hist
    h1 = hist[-2] if len(hist) > 1 else 0  # previous hist, viết thêm if len(hist) > 1 else 0 để tránh crash và lỗi IndexError khi danh sách chưa về đủ phần tử hist thứ 2
    current_high = highs[-1]
    current_low = lows[-1]

    # ============== CLASSIC BEARISH DIVERGENCE ==============
    # Track sequence dương (ZoneA cho bearish)
    Classic_pos_sequence_start = h0 >= 0 and h1 < 0
    Classic_pos_sequence_end = h0 < 0 and h1 >= 0

    # Nếu Classic_pos_sequence_start thì khởi tạo và cập nhật các giá trị cao nhất
    if Classic_pos_sequence_start:
        state.Classic_max_high_in_sequence = current_high
        state.Classic_max_hist_in_sequence = h0
        state.Classic_bearZoneA_hist_highest_barIndex = bar_index
    elif h0 >= 0:  # Tiếp tục vùng hist >= 0
        if current_high >= state.Classic_max_high_in_sequence:
            state.Classic_max_high_in_sequence = current_high
        if h0 >= state.Classic_max_hist_in_sequence:
            state.Classic_max_hist_in_sequence = h0
            state.Classic_bearZoneA_hist_highest_barIndex = bar_index

    # Kết thúc ZoneA khi hist chuyển từ dương sang âm và lưu lại các giá trị cao nhất
    if Classic_pos_sequence_end:
        state.Classic_bearZoneA_high_highest = state.Classic_max_high_in_sequence
        state.Classic_bearZoneA_hist_highest = state.Classic_max_hist_in_sequence
        state.Classic_bearZoneA_bar_highest = state.Classic_bearZoneA_hist_highest_barIndex
        state.Classic_in_bearZoneB = False
        state.Classic_bearZoneB_occurred = False

    # ZoneB = Vùng histogram âm
    if not math.isnan(state.Classic_bearZoneA_hist_highest) and h0 < 0 and not state.Classic_bearZoneB_occurred:
        state.Classic_in_bearZoneB = True

    # Kết thúc ZoneB khi hist >= 0
    if state.Classic_in_bearZoneB and h0 >= 0:
        state.Classic_bearZoneB_occurred = True
        state.Classic_in_bearZoneB = False

    # Tính số bar từ đỉnh hist ZoneA
    bars_since_Classic_bearZoneA_hist_highest = (bar_index - state.Classic_bearZoneA_bar_highest 
                                                if not math.isnan(state.Classic_bearZoneA_bar_highest) else 0)

    # Tín hiệu bearish classic
    signals["bearish"] = (
        not math.isnan(state.Classic_bearZoneA_hist_highest) and
        state.Classic_bearZoneB_occurred and
        current_high > state.Classic_bearZoneA_high_highest and
        h0 <= slopeThreshold * state.Classic_bearZoneA_hist_highest and
        bars_since_Classic_bearZoneA_hist_highest >= min_lookbackBars and
        bars_since_Classic_bearZoneA_hist_highest <= max_lookbackBars
    )

    # Nếu hist hiện tại phá luôn slopeThreshold * đỉnh hist ZoneA → huỷ
    if (not math.isnan(state.Classic_bearZoneA_hist_highest) and 
        h0 > slopeThreshold * state.Classic_bearZoneA_hist_highest):
        state.Classic_bearZoneA_high_highest = math.nan
        state.Classic_bearZoneA_hist_highest = math.nan
        state.Classic_bearZoneA_hist_highest_barIndex = math.nan
        state.Classic_bearZoneB_occurred = False
        state.Classic_in_bearZoneB = False

    # ============== CLASSIC BULLISH DIVERGENCE ==============
    # Track sequence âm (ZoneA cho bullish)
    Classic_neg_sequence_start = h0 < 0 and h1 >= 0
    Classic_neg_sequence_end = h0 >= 0 and h1 < 0

    # Nếu neg_sequence_start thì khởi tạo ZoneA và cập nhật các giá trị thấp nhất
    if Classic_neg_sequence_start:
        state.Classic_min_low_in_sequence = current_low
        state.Classic_min_hist_in_sequence = h0
        state.Classic_bullZoneA_hist_lowest_barIndex = bar_index
    elif h0 < 0:  # Tiếp tục vùng hist < 0
        if current_low <= state.Classic_min_low_in_sequence:
            state.Classic_min_low_in_sequence = current_low
        if h0 <= state.Classic_min_hist_in_sequence:
            state.Classic_min_hist_in_sequence = h0
            state.Classic_bullZoneA_hist_lowest_barIndex = bar_index

    # Kết thúc ZoneA khi hist chuyển từ âm sang dương và lưu lại các giá trị thấp nhất
    if Classic_neg_sequence_end:
        state.Classic_bullZoneA_low_lowest = state.Classic_min_low_in_sequence
        state.Classic_bullZoneA_hist_lowest = state.Classic_min_hist_in_sequence
        state.Classic_bullZoneA_bar_lowest = state.Classic_bullZoneA_hist_lowest_barIndex
        state.Classic_in_bullZoneB = False
        state.Classic_bullZoneB_occurred = False

    # ZoneB = Vùng histogram dương
    if not math.isnan(state.Classic_bullZoneA_hist_lowest) and h0 >= 0 and not state.Classic_bullZoneB_occurred:
        state.Classic_in_bullZoneB = True

    # Kết thúc ZoneB khi hist <= 0
    if state.Classic_in_bullZoneB and h0 <= 0:
        state.Classic_bullZoneB_occurred = True
        state.Classic_in_bullZoneB = False

    # Tính số bar từ đáy hist ZoneA
    bars_since_Classic_bullZoneA_hist_lowest = (bar_index - state.Classic_bullZoneA_bar_lowest 
                                               if not math.isnan(state.Classic_bullZoneA_bar_lowest) else 0)

    signals["bullish"] = (
        not math.isnan(state.Classic_bullZoneA_hist_lowest) and
        state.Classic_bullZoneB_occurred and
        current_low < state.Classic_bullZoneA_low_lowest and
        h0 >= slopeThreshold * state.Classic_bullZoneA_hist_lowest and
        bars_since_Classic_bullZoneA_hist_lowest >= min_lookbackBars and
        bars_since_Classic_bullZoneA_hist_lowest <= max_lookbackBars
    )

    # Nếu hist hiện tại phá luôn slopeThreshold * đáy hist ZoneA → huỷ
    if (not math.isnan(state.Classic_bullZoneA_hist_lowest) and 
        h0 < slopeThreshold * state.Classic_bullZoneA_hist_lowest):
        state.Classic_bullZoneA_low_lowest = math.nan
        state.Classic_bullZoneA_hist_lowest = math.nan
        state.Classic_bullZoneA_bar_lowest = math.nan
        state.Classic_bullZoneB_occurred = False
        state.Classic_in_bullZoneB = False

    return signals

# ============== DETECTION MISS SHOULDER DIVERGENCE LOGIC ===============
def detect_miss_shoulder_divergence(state: MissShoulderDivergenceState, closes, highs, lows, hist, bar_index, min_lookbackBars, max_lookbackBars):
    signals = {"bearish": False, "bullish": False}
    if len(hist) < 2:
        return signals

    h0 = hist[-1]  # current hist
    h1 = hist[-2] if len(hist) > 1 else 0  # previous hist
    current_high = highs[-1]
    current_low = lows[-1]

    # ============== MISS SHOULDER BEARISH DIVERGENCE ==============
    # Track sequence dương (ZoneA cho bearish)
    MS_pos_sequence_start = h0 >= 0 and h1 < 0
    MS_pos_sequence_end = h0 < 0 and h1 >= 0

    # Nếu pos_sequence_start thì khởi tạo ZoneA và lưu các giá trị cao nhất
    if MS_pos_sequence_start:
        state.MS_max_high_in_sequence = current_high
        state.MS_max_hist_in_sequence = h0
        state.MS_bearZoneA_hist_highest_barIndex = bar_index
    elif h0 >= 0:  # Tiếp tục vùng hist >= 0
        if current_high >= state.MS_max_high_in_sequence:
            state.MS_max_high_in_sequence = current_high
        if h0 >= state.MS_max_hist_in_sequence:
            state.MS_max_hist_in_sequence = h0
            state.MS_bearZoneA_hist_highest_barIndex = bar_index

    # Khi kết thúc ZoneA, lưu lại các giá trị cao nhất và reset trạng thái
    if MS_pos_sequence_end:
        state.MS_bearZoneA_high_highest = state.MS_max_high_in_sequence
        state.MS_bearZoneA_hist_highest = state.MS_max_hist_in_sequence
        state.MS_bearZoneA_bar_highest = state.MS_bearZoneA_hist_highest_barIndex
        state.MS_in_bearZoneB = False
        state.MS_bearZoneB_occurred = False

    # ZoneB bắt đầu khi hist chuyển sang âm sau zoneA
    if not math.isnan(state.MS_bearZoneA_hist_highest) and h0 < 0 and not state.MS_bearZoneB_occurred:
        state.MS_in_bearZoneB = True

    # ZoneB kết thúc khi hist bắt đầu thể hiện sự đảo chiều (hist [-1] > hist[-2] theo Python, hist[0] > hist[1] theo Pine Script)
    if state.MS_in_bearZoneB and h0 > h1:
        state.MS_bearZoneB_occurred = True
        state.MS_in_bearZoneB = True

    # Tính số bar từ đỉnh hist ZoneA
    bars_since_MS_bearZoneA_hist_highest = (bar_index - state.MS_bearZoneA_bar_highest 
                                           if not math.isnan(state.MS_bearZoneA_bar_highest) else 0)

    # Tín hiệu bearish Miss Shoulder
    signals["bearish"] = (
        not math.isnan(state.MS_bearZoneA_hist_highest) and
        state.MS_bearZoneB_occurred and
        current_high > state.MS_bearZoneA_high_highest and
        h0 < 0 and
        h0 > h1 and
        bars_since_MS_bearZoneA_hist_highest >= min_lookbackBars and
        bars_since_MS_bearZoneA_hist_highest <= max_lookbackBars
    )

    # ============== MISS SHOULDER BULLISH DIVERGENCE ==============
    # Track sequence âm (ZoneA cho bullish)
    MS_neg_sequence_start = h0 < 0 and h1 >= 0
    MS_neg_sequence_end = h0 >= 0 and h1 < 0

    # Nếu MS_neg_sequence_start thì khởi tạo ZoneA và lưu các giá trị thấp nhất
    if MS_neg_sequence_start:
        state.MS_min_low_in_sequence = current_low
        state.MS_min_hist_in_sequence = h0
        state.MS_bullZoneA_hist_lowest_barIndex = bar_index
    elif h0 < 0:  # Tiếp tục vùng hist < 0
        if current_low <= state.MS_min_low_in_sequence:
            state.MS_min_low_in_sequence = current_low
        if h0 <= state.MS_min_hist_in_sequence:
            state.MS_min_hist_in_sequence = h0
            state.MS_bullZoneA_hist_lowest_barIndex = bar_index

    # Khi kết thúc ZoneA, lưu lại các giá trị thấp nhất và reset trạng thái
    if MS_neg_sequence_end:
        state.MS_bullZoneA_low_lowest = state.MS_min_low_in_sequence
        state.MS_bullZoneA_hist_lowest = state.MS_min_hist_in_sequence
        state.MS_bullZoneA_bar_lowest = state.MS_bullZoneA_hist_lowest_barIndex
        state.MS_in_bullZoneB = False
        state.MS_bullZoneB_occurred = False

    # ZoneB bắt đầu khi hist chuyển sang dương
    if (not math.isnan(state.MS_bullZoneA_hist_lowest) and 
        h0 >= 0 and 
        not state.MS_bullZoneB_occurred):
        state.MS_in_bullZoneB = True

    # ZoneB kết thúc khi hist bắt đầu thể hiện sự đảo chiều (hist[-1] < hist[-2] theo Python, hist[0] < hist[1] theo Pine Script)
    if state.MS_in_bullZoneB and h0 < h1:
        state.MS_bullZoneB_occurred = True
        state.MS_in_bullZoneB = True

    bars_since_MS_bullZoneA_hist_lowest = (bar_index - state.MS_bullZoneA_bar_lowest 
                                          if not math.isnan(state.MS_bullZoneA_bar_lowest) else 0)

    # Tín hiệu bullish Miss Shoulder
    signals["bullish"] = (
        not math.isnan(state.MS_bullZoneA_hist_lowest) and
        state.MS_bullZoneB_occurred and
        current_low < state.MS_bullZoneA_low_lowest and
        h0 >= 0 and
        h0 < h1 and
        bars_since_MS_bullZoneA_hist_lowest >= min_lookbackBars and
        bars_since_MS_bullZoneA_hist_lowest <= max_lookbackBars
    )

    return signals

def detect_signals(state: DivergenceState, symbol, timeframe, closes, highs, lows, hist, bar_id):
    out = []
    if len(closes) < 3 or len(hist) < 3:
        return out

    bar_index = len(closes) - 1  # Tương đương bar_index trong Pine Script

    # Classic Divergence Detection
    classic_signals = detect_classic_divergence(
        state.classic, 
        closes, highs, lows, hist, 
        bar_index, MIN_LOOKBACK_BARS, MAX_LOOKBACK_BARS, SLOPE_THRESHOLD
    )

    # Miss Shoulder Divergence Detection
    miss_shoulder_signals = detect_miss_shoulder_divergence(
        state.miss_shoulder,
        closes, highs, lows, hist,
        bar_index, MIN_LOOKBACK_BARS, MAX_LOOKBACK_BARS
    )

    # Check for new signals
    if classic_signals["bearish"] and state.last_bar_id_bearish_classic != bar_id:
        out.append(f"🚨 {symbol}, {timeframe}, Bearish Divergence Signal (Classic)")
        state.last_bar_id_bearish_classic = bar_id

    if classic_signals["bullish"] and state.last_bar_id_bullish_classic != bar_id:
        out.append(f"🚨 {symbol}, {timeframe}, Bullish Divergence Signal (Classic)")
        state.last_bar_id_bullish_classic = bar_id

    if miss_shoulder_signals["bearish"] and state.last_bar_id_bearish_ms != bar_id:
        out.append(f"🚨 {symbol}, {timeframe}, Bearish Divergence Signal (Miss Shoulder)")
        state.last_bar_id_bearish_ms = bar_id

    if miss_shoulder_signals["bullish"] and state.last_bar_id_bullish_ms != bar_id:
        out.append(f"🚨 {symbol}, {timeframe}, Bullish Divergence Signal (Miss Shoulder)")
        state.last_bar_id_bullish_ms = bar_id

    return out

# ================== CTRADER OPEN API DATAFEED ==================

# ================== CONFIGURATION ==================
# Thông tin tài khoản cTrader và API
CTRADER_ACCOUNT_ID = "9560393"
CTRADER_CLIENT_ID = "16778_at26WTcoFS2NYt1GHQS1gJqaSWorbHZCJKA1X9KRe2z5dZRrMo"
CTRADER_SECRET = "unq1iRL42CtmzTk5MQ9CYdcMfnYmOQSV5Nfu94FEX0ZueystC3"
CTRADER_ACCESS_TOKEN = "Ztq2opUDDRln67kqczqVC9Giml3xNjmCru46I5BstwU"
CTRADER_REFRESH_TOKEN = "F7bAEc2QBk-5Sf8J0T3MUyRw9D6neWxKvq29LtGZmYY"
CTRADER_DEMO_HOST = "demo.ctraderapi.com"
CTRADER_DEMO_PORT = 5035
CTRADER_TOKEN_URL = "https://openapi.ctrader.com/apps/token"

# ================== EXCEPTIONS ==================
class APIError(Exception):
    """Custom exception cho các lỗi API cTrader"""
    pass

# ================== REAL-TIME FEED CLASS ==================
class DataFeed:
    def __init__(self, bot_instance=None):
        self.client = None
        self.connected = False
        self.access_token = CTRADER_ACCESS_TOKEN
        self.refresh_token = CTRADER_REFRESH_TOKEN
        self.token_expires = time.time() + 2628000  # 30 ngày
        self.bot_instance = bot_instance
        self.account_id = None
        self.subscriptions = {}  # key=(symbol,timeframe), value=callback

        # Symbol mapping
        self.symbol_mapping = {
            "XAUUSD": "XAUUSD", "EURUSD": "EURUSD", "EURAUD": "EURAUD", "EURCAD": "EURCAD",
            "EURCHF": "EURCHF", "EURGBP": "EURGBP", "EURNZD": "EURNZD", "GBPUSD": "GBPUSD",
            "GBPAUD": "GBPAUD", "GBPCAD": "GBPCAD", "GBPCHF": "GBPCHF", "GBPNZD": "GBPNZD",
            "AUDUSD": "AUDUSD", "AUDCAD": "AUDCAD", "AUDCHF": "AUDCHF", "AUDNZD": "AUDNZD",
            "CADCHF": "CADCHF", "USDCHF": "USDCHF", "USDCAD": "USDCAD",
            "NZDUSD": "NZDUSD", "NZDCAD": "NZDCAD", "NZDCHF": "NZDCHF"
        }

        # Timeframe mapping
        self.tf_mapping = {
            "5m": OAModel.ProtoTrendbarPeriod.M5,
            "15m": OAModel.ProtoTrendbarPeriod.M15,
            "30m": OAModel.ProtoTrendbarPeriod.M30,
            "1h": OAModel.ProtoTrendbarPeriod.H1,
            "4h": OAModel.ProtoTrendbarPeriod.H4,
            "1d": OAModel.ProtoTrendbarPeriod.D1
        }

    # ------------------- Helper -------------------
    async def _send_error(self, msg: str):
        logging.error(msg)
        if self.bot_instance:
            await self.bot_instance.send(f"⚠️ **API ERROR**\n\n{msg}\n⏸️ Bot paused.")

    async def _refresh_token_if_needed(self):
        if time.time() < self.token_expires - 300:
            return
        import aiohttp
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token,
                    "client_id": CTRADER_CLIENT_ID,
                    "client_secret": CTRADER_SECRET
                }
                async with session.post(CTRADER_TOKEN_URL, json=payload) as resp:
                    if resp.status != 200:
                        raise APIError(await resp.text())
                    data = await resp.json()
                    self.access_token = data["access_token"]
                    self.refresh_token = data.get("refresh_token", self.refresh_token)
                    self.token_expires = time.time() + data["expires_in"]
                    logging.info("✅ Access token refreshed")
        except Exception as e:
            await self._send_error(f"Token refresh failed: {e}")
            raise

    # ------------------- Connect -------------------
    def connect(self):
        d = defer.Deferred()

        async def _do_connect():
            await self._refresh_token_if_needed()
            self.client = Client(CTRADER_DEMO_HOST, CTRADER_DEMO_PORT, TcpProtocol)

            # Authenticate app
            auth_req = OA.ProtoOAApplicationAuthReq()
            auth_req.clientId = CTRADER_CLIENT_ID
            auth_req.clientSecret = CTRADER_SECRET
            auth_res = await self.client.send(auth_req)
            if not isinstance(auth_res, OA.ProtoOAApplicationAuthRes):
                raise APIError("App authentication failed")

            # Authenticate account
            account_req = OA.ProtoOAAccountAuthReq()
            account_req.ctidTraderAccountId = int(CTRADER_ACCOUNT_ID)
            account_req.accessToken = self.access_token
            account_res = await self.client.send(account_req)
            if not isinstance(account_res, OA.ProtoOAAccountAuthRes):
                raise APIError("Account authentication failed")

            self.account_id = account_res.ctidTraderAccountId
            self.connected = True
            logging.info(f"✅ Connected to account {self.account_id}")
            self.client.setMessageReceivedCallback(self._on_message)
            d.callback(True)

        from twisted.internet import asyncioreactor
        # Chạy async function trong reactor
        asyncioreactor.AsyncioSelectorReactor().callLater(0, lambda: asyncio.ensure_future(_do_connect()))
        return d

    # ------------------- Subscribe -------------------
    def subscribe_candle(self, symbol, timeframe, callback):
        if not self.connected:
            d = self.connect()
            d.addCallback(lambda _: self._do_subscribe(symbol, timeframe, callback))
            d.addErrback(self._on_error)
            return d
        else:
            return self._do_subscribe(symbol, timeframe, callback)
            
    # ------------------- Get candles Twisted -------------------
    def get_candles (self, symbol, timeframe, lookback, callback):
        lookback = MIN_BARS_BACK
        d = defer.Deferred()
        candles = deque(maxlen=lookback)

        def _internal_callback(candle):
            candles.append(candle)
            if len(candles) >= lookback:
                if not d.called:
                    d.callback(list(candles))
                    reactor.callLater(0, lambda: self.subscriptions.pop((symbol.upper(), timeframe.lower()), None))

        reactor.callLater(0, lambda: self.subscribe_candle(symbol, timeframe, _internal_callback))
        d.addCallback(lambda lst: callback(lst))
        reactor.callLater(5, lambda: d.callback(list(candles)) if not d.called else None)
        return d
        
    def _do_subscribe(self, symbol, timeframe, callback):
        key = (symbol.upper(), timeframe.lower())
        self.subscriptions[key] = callback

        sym_req = OA.ProtoOASymbolsListReq()
        sym_req.ctidTraderAccountId = self.account_id
        d = self.client.send(sym_req)

        def got_symbols(res):
            if not isinstance(res, OA.ProtoOASymbolsListRes):
                raise Exception("Failed to get symbols")
            ctr_symbol = self.symbol_mapping.get(symbol.upper(), symbol.upper())
            symbol_id = next((s.symbolId for s in res.symbol if s.symbolName == ctr_symbol), None)
            if symbol_id is None:
                raise Exception(f"Symbol {ctr_symbol} not found")
            sub_req = OA.ProtoOASubscribeTrendbarsReq()
            sub_req.ctidTraderAccountId = self.account_id
            sub_req.symbolId = symbol_id
            sub_req.period = self.tf_mapping.get(timeframe.lower(), OAModel.ProtoTrendbarPeriod.M5)
            return self.client.send(sub_req)

        d.addCallback(got_symbols)
        d.addCallback(lambda _: logging.info(f"📈 Subscribed candles {symbol} {timeframe}"))
        d.addErrback(self._on_error)
        return d

    # ------------------- Message Handler -------------------
    def _parse_trendbar_event(self, trendbar_event, symbol):
        tb = trendbar_event.trendbar
        rnd = 2 if symbol.upper().startswith("XAU") else 3 if symbol.upper().endswith("JPY") else 5
        return {
            "t": int(tb.timestamp) // 1000,
            "o": round(tb.open / 100000, rnd),
            "h": round(tb.high / 100000, rnd),
            "l": round(tb.low / 100000, rnd),
            "c": round(tb.close / 100000, rnd),
            "v": int(tb.volume)
        }

    def _on_message(self, client, msg):
        if msg.payloadType == OA.ProtoOATrendbarEvent().payloadType:
            symbol_name = msg.trendbar.symbolName
            for (sym, tf), callback in self.subscriptions.items():
                if sym == symbol_name:
                    from twisted.internet import task
                    task.react(lambda _: callback(self._parse_trendbar_event(msg, symbol_name)))

    # ------------------- Close -------------------
    def close(self):
        if self.client and self.connected:
            d = self.client.disconnect()
            self.connected = False
            logging.info("🔌 Disconnected from cTrader")
            return d
            
def handle_candles(candles, sym, tf):
    print(f"{sym} {tf} nhận nến:", candles)
    
feed = DataFeed()
for sym in PAIRS:
    for tf in PRESETS:
        feed.get_candles(sym, tf, MIN_BARS_BACK, lambda c: handle_candles(c, sym, tf))

# ================== TELEGRAM BOT ==================

# Sử dụng MIN_BARS_BACK từ config để đảm bảo đủ dữ liệu cho divergence detection
MIN_BARS = MIN_BARS_BACK
class DivergenceBot:
    def __init__(self):
        self.session = None
        self.offset = None
        self.running = False
        self.selected_timeframes = []
        self.states = defaultdict(DivergenceState)
        # History buffer: maxlen=MIN_BARS đủ cho MAX_LOOKBACK_BARS (40) và có dư để tính MACD
        self.history = defaultdict(lambda: {
            "o": deque(maxlen=MIN_BARS),
            "h": deque(maxlen=MIN_BARS),
            "l": deque(maxlen=MIN_BARS),
            "c": deque(maxlen=MIN_BARS)
        })
        from telegram import Bot as TgBot
        # Khởi tạo bot Telegram
        self.bot_instance = TgBot(token="TELEGRAM_TOKEN")
        # Khởi tạo DataFeed và gán vào self.feed
        self.feed = DataFeed(bot_instance=self.bot_instance)
        
    async def send_startup_message(self):
        """Gửi thông báo khi bot khởi động"""
        startup_msg = (
            "🤖 **MACD Divergence Bot v2.0 STARTED!**\n\n"
            f"⏰ **Time:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"📡 **Data Source:** IC Markets Demo\n"
            f"📈 **Monitoring:** {len(PAIRS)} currency pairs\n"
            f"🔧 **MACD:** {MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}\n\n"
            f"💡 **Type `/help` for instructions**\n"
            f"🚀 **Ready to scan for divergences!**"
            )
        await self.send(startup_msg)
    
    def reset_scan_state(self):
        """Reset trạng thái scan để bắt đầu mới - KHÔNG tạo DataFeed mới"""
        # Chỉ reset states và history, giữ lại DataFeed instance
        self.states = defaultdict(DivergenceState)
        self.history = defaultdict(lambda: {
            "o": deque(maxlen=MIN_BARS),
            "h": deque(maxlen=MIN_BARS),
            "l": deque(maxlen=MIN_BARS),
            "c": deque(maxlen=MIN_BARS)
        })
        # DataFeed được giữ lại để tránh memory leak và maintain connection
        logging.info("🔄 Scan state reset - DataFeed connection maintained")

    async def _ensure_session(self):
        """Đảm bảo session tồn tại"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) #Nếu 30s không có phản hồi thì bot dừng

    async def send(self, text: str):
        """Gửi message qua Telegram"""
        await self._ensure_session()
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

        # Split message if too long (Telegram limit 4096 chars)
        max_length = 4000
        if len(text) > max_length:
            parts = [text[i:i+max_length] for i in range(0, len(text), max_length)]
            for part in parts:
                async with self.session.post(url, data={
                    "chat_id": CHAT_ID, 
                    "text": part,
                    "parse_mode": "Markdown"
                }) as resp:
                    if resp.status != 200:
                        logging.warning("Failed to send Telegram message: %s", await resp.text())
                    await asyncio.sleep(0.5)  # Avoid rate limiting
        else:
            async with self.session.post(url, data={
                "chat_id": CHAT_ID, 
                "text": text,
                "parse_mode": "Markdown"
            }) as resp:
                if resp.status != 200:
                    logging.warning("Failed to send Telegram message: %s", await resp.text())

    async def fetch_updates(self):
        """Lấy updates từ Telegram"""
        await self._ensure_session()
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 50}
        if self.offset:
            params["offset"] = self.offset

        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("result", [])
                else:
                    logging.warning(f"Failed to fetch updates: {resp.status}")
                    return []
        except Exception as e:
            logging.error(f"Error fetching updates: {e}")
            return []

    async def handle_updates(self):
        """Xử lý updates từ Telegram"""
        updates = await self.fetch_updates()
        for u in updates:
            self.offset = u["update_id"] + 1
            msg = u.get("message") or u.get("edited_message")
            if not msg: 
                continue

            chat_id = str(msg["chat"]["id"])
            text = (msg.get("text") or "").strip()
            user = msg.get("from", {})
            username = user.get("username", "Unknown")
            first_name = user.get("first_name", "")

            if not text: 
                continue

            # Log received message
            logging.info(f"📨 Received from @{username} ({first_name}): {text}")

            # Echo message back (feature bạn yêu cầu thêm)
            if not text.startswith("/"):
                echo_msg = f"📢 **Echo từ @{username}:** {text}"
                await self.send(echo_msg)
                continue

            await self.handle_command(text, username, first_name)

    async def handle_command(self, text: str, username: str = "", first_name: str = ""):
        """Xử lý các lệnh từ user"""
        t = text.strip()
        low = t.lower()

        # Command: /id - Show current chat ID
        if low.startswith("/id"):
            await self.send(f"🆔 **Current CHAT_ID:** `{CHAT_ID}`\n👤 **Your Username:** @{username}")
            return

        # Command: /start hoặc /help - Hướng dẫn sử dụng
        if low.startswith("/start") or low.startswith("/help"):
            help_text = (
                "🤖 **MACD Divergence Detection Bot v2.0**\n\n"
                "📈 **Tính năng chính:**\n"
                "• Phát hiện phân kỳ MACD Classic & Miss Shoulder\n"
                "• Theo dõi realtime từ IC Markets\n"
                "• Hỗ trợ 23 cặp tiền chính\n"
                "• Đa timeframe scanning\n\n"
                "🛠️ **Available Commands:**\n"
                "/help - Hướng dẫn & hỗ trợ\n"
                "/status - Kiểm tra trạng thái bot\n"
                "/stop - Dừng scanning\n"
                "/pairs - Danh sách cặp tiền theo dõi\n"
                "/scan [timeframe] - Bắt đầu quét\n\n"
                "📊 **Scan Commands:**\n"
                "**Presets:**\n"
                "`/scan 4T` - 5m,15m,30m,1h\n"
                "`/scan 2t` - 5m,15m\n"
                "`/scan 2T` - 30m,1h\n\n"
                "**Single timeframes:**\n"
                "`/scan 5m, /scan 15m, /scan 30m`\n"
                "`/scan 1h, /scan 4h, /scan 1d`\n\n"
                "💡 **Tip:** Gửi tin nhắn thường để test Echo feature!"
            )
            await self.send(help_text)
            return

        # Command: /status - Kiểm tra trạng thái
        if low.startswith("/status"):
            status_emoji = "🟢" if self.running else "🔴"
            status_text = "RUNNING" if self.running else "STOPPED"

            tf_list = ", ".join(self.selected_timeframes) if self.selected_timeframes else "None"

            status_msg = (
                f"📊 **Bot Status:** {status_emoji} {status_text}\n\n"
                f"⏱️ **Timeframes:** {tf_list}\n"
                f"🔄 **Scan Interval:** {SCAN_INTERVAL_SEC}s\n"
                f"📈 **Total Pairs:** {len(PAIRS)}\n"
                f"📡 **Data Source:** IC Markets (cTrader)\n\n"
                f"🧮 **MACD Settings:**\n"
                f"• Fast EMA: {MACD_FAST}\n"
                f"• Slow EMA: {MACD_SLOW}\n"
                f"• Signal: {MACD_SIGNAL}\n\n"
                f"🔍 **Divergence Settings:**\n"
                f"• Min Lookback: {MIN_LOOKBACK_BARS} bars\n"
                f"• Max Lookback: {MAX_LOOKBACK_BARS} bars\n"
                f"• Slope Threshold: {SLOPE_THRESHOLD}\n\n"
                f"📱 **User:** @{username} ({first_name})"
            )
            await self.send(status_msg)
            return

        # Command: /stop - Dừng scanning
        if low.startswith("/stop"):
            if self.running:
                self.running = False
                self.selected_timeframes = []
                await self.send(f"🛑 **Scanning STOPPED** by @{username}\n\n⏸️ Bot đã dừng hoạt động. Sử dụng `/scan` để khởi động lại.")
            else:
                await self.send("ℹ️ Bot đã dừng từ trước. Sử dụng `/scan` để bắt đầu.")
            return

        # Command: /pairs - Danh sách cặp tiền
        if low.startswith("/pairs"):
            pairs_list = []
            for i, pair in enumerate(PAIRS, 1):
                pairs_list.append(f"{i:2d}) {pair}")

            # Group pairs by 10
            grouped_pairs = []
            for i in range(0, len(pairs_list), 10):
                group = pairs_list[i:i+10]
                grouped_pairs.append("\n".join(group))

            pairs_text = (
                f"📊 **Danh sách {len(PAIRS)} cặp tiền đang theo dõi:**\n\n"
                f"```\n{grouped_pairs[0]}\n```\n\n"
            )

            if len(grouped_pairs) > 1:
                pairs_text += f"```\n{grouped_pairs[1]}\n```\n\n"

            if len(grouped_pairs) > 2:
                pairs_text += f"```\n{grouped_pairs[2]}\n```\n\n"

            pairs_text += (
                f"💡 **Data Source:** IC Markets Demo Server\n"
                f"🚀 Sử dụng lệnh `/scan [timeframe]` để bắt đầu quét phân kỳ!"
            )
            await self.send(pairs_text)
            return

        # Command: /scan - Bắt đầu scanning
        if low.startswith("/scan"):
            parts = t.split()
            if len(parts) == 1:
                scan_help = (
                    "⚠️ **Vui lòng chỉ định timeframe:**\n\n"
                    "📊 **Preset Options:**\n"
                    "`/scan 4T` - Scan 4 timeframes (5m,15m,30m,1h)\n"
                    "`/scan 2t` - Scan 2 timeframes thấp (5m,15m)\n"
                    "`/scan 2T` - Scan 2 timeframes cao (30m,1h)\n\n"
                    "⏰ **Single Timeframes:**\n"
                    "`/scan 5m` - Chỉ scan 5 phút\n"
                    "`/scan 15m` - Chỉ scan 15 phút\n"
                    "`/scan 30m` - Chỉ scan 30 phút\n"
                    "`/scan 1h` - Chỉ scan 1 giờ\n"
                    "`/scan 4h` - Chỉ scan 4 giờ\n"
                    "`/scan 1d` - Chỉ scan 1 ngày\n\n"
                    "💡 **Ví dụ:** `/scan 4T` hoặc `/scan 15m`"
                )
                await self.send(scan_help)
                return

            key = parts[1].lower()
            valid_single_tf = ["5m", "15m", "30m", "1h", "4h", "1d"]

            # Handle single timeframes
            if key in valid_single_tf:
                self.reset_scan_state()
                self.selected_timeframes = [key]
                self.running = True

                start_msg = (
                    f"🚀 **SCAN STARTED** by @{username}\n\n"
                    f"⏰ **Timeframe:** {key.upper()}\n"
                    f"📈 **Pairs:** {len(PAIRS)} cặp tiền\n"
                    f"🔄 **Interval:** {SCAN_INTERVAL_SEC} giây\n"
                    f"📡 **Source:** IC Markets Demo\n\n"
                    f"🎯 Bot sẽ thông báo khi phát hiện phân kỳ MACD!"
                )
                await self.send(start_msg)
                return

            # Handle presets
            preset_key = key.lower() if key in ["4t", "2t", "2T"] else key
            if preset_key in PRESETS:
                self.reset_scan_state()
                self.selected_timeframes = PRESETS[preset_key]
                self.running = True

                timeframes_display = ", ".join([tf.upper() for tf in self.selected_timeframes])
                total_scans = len(self.selected_timeframes) * len(PAIRS)

                start_msg = (
                    f"🚀 **MULTI-TIMEFRAME SCAN STARTED** by @{username}\n\n"
                    f"⏰ **Timeframes:** {timeframes_display}\n"
                    f"📈 **Total Scans:** {total_scans} (Pairs: {len(PAIRS)})\n"
                    f"🔄 **Scan Interval:** {SCAN_INTERVAL_SEC} giây\n"
                    f"📡 **Data Source:** IC Markets Demo\n\n"
                    f"🎯 Bot sẽ thông báo phân kỳ trên tất cả timeframes!"
                )
                await self.send(start_msg)
                return

            # Invalid timeframe
            error_msg = (
                "❌ **Invalid timeframe!**\n\n"
                "✅ **Valid Options:**\n\n"
                "**Presets:** 4T, 2t, 2T\n"
                "**Single:** 5m, 15m, 30m, 1h, 4h, 1d\n\n"
                "💡 Sử dụng `/help` để xem hướng dẫn chi tiết"
            )
            await self.send(error_msg)
            return

        # Unknown command
        await self.send(f"❓ **Unknown command:** `{text}`\n\n💡 Type `/help` for instructions")

    def bar_id(self, ts_unix: int, timeframe: str) -> int:
        """Tạo unique bar ID dựa trên timestamp và timeframe"""
        tf_min = tf_to_minutes(timeframe)
        return int(ts_unix // (tf_min * 60))

    async def scan_once(self):
        """Thực hiện một lần scan tất cả pairs và timeframes"""
        if not self.running or not self.selected_timeframes:
            return

        scan_start_time = time.time()
        total_signals = 0

        for tf in self.selected_timeframes:
            for symbol in PAIRS:
                key = f"{symbol}|{tf}"
                lookback = MIN_BARS

                try:
                    # Get candle data từ cTrader
                    def handle_scan(candles, symbol, tf):
                        # Update history, tính MACD, detect divergence, send signals
                        ...
                    self.feed.get_candles(symbol, tf, MIN_BARS, lambda c, s=symbol, t=tf: handle_scan(c, s, t))

                except APIError as e:
                    logging.error(f"API Error for {symbol} {tf}: {e}")
                    # Bot sẽ tự động gửi error notification thông qua DataFeed
                    await asyncio.sleep(0.1)
                    continue

                except Exception as e:
                    logging.warning(f"Unexpected error for {symbol} {tf}: {e}")
                    await asyncio.sleep(0.05)
                    continue

                # Dữ liệu nến hợp lệ 
                if not candles or len(candles) < MIN_BARS:
                    logging.debug(f"Không đủ dữ liệu cho {symbol} {tf}: {len(candles) if candles else 0} candles")
                    continue

                # Sắp xếp nến theo thứ tự thời gian cũ - mới
                candles = sorted(candles, key=lambda x: x["t"])

                # Update history
                histo = self.history[key]
                histo["o"].clear()
                histo["h"].clear()
                histo["l"].clear()
                histo["c"].clear()

                for c in candles:
                    histo["o"].append(c["o"])
                    histo["h"].append(c["h"])
                    histo["l"].append(c["l"])
                    histo["c"].append(c["c"])

                # Extract OHLC arrays
                closes = list(histo["c"])
                highs = list(histo["h"])
                lows = list(histo["l"])

                # Calculate MACD Histogram (Đủ 3 data hist mới xét được phân kỳ, hàm continue cho tới khi đủ)
                hist_values = macd_hist(closes)
                if len(hist_values) < 3:
                    continue

                # Generate bar ID cho anti-spam
                barid = self.bar_id(candles[-1]["t"], tf)

                # Detect divergence signals
                state = self.states[key]
                signals = detect_signals(state, symbol, tf, closes, highs, lows, hist_values, barid)

                # Send signals
                for signal in signals:
                    await self.send(signal)
                    total_signals += 1 # Đếm tổng số signal mỗi lần
                    logging.info(f"🚨 Signal sent: {signal}")

                # Độ trễ nhỏ để tránh API quá tải
                await asyncio.sleep(0.02)

        scan_duration = time.time() - scan_start_time
        logging.debug(f"Scan completed in {scan_duration:.2f}s, {total_signals} signals sent")

    async def run(self):
        """Main bot loop"""
        #keep_alive()  # Start web server for hosting

        try:
            # Send startup notification
            await self.send_startup_message()

            # Main loop
            while True:
                try:
                    # Handle Telegram updates với timeout
                    await asyncio.wait_for(self.handle_updates(), timeout=1)
                except asyncio.TimeoutError:
                    pass  # Normal timeout, continue
                except Exception as e:
                    logging.error(f"❌ Error in handle_updates: {e}")
                    await asyncio.sleep(1)

                try:
                    # Scan for divergences
                    await self.scan_once()
                except Exception as e:
                    logging.error(f"❌ Error in scan_once: {e}")
                    # Nếu có lỗi nghiêm trọng, tạm dừng scanning
                    if "API" in str(e) or "Connection" in str(e):
                        self.running = False
                        await self.send(f"⚠️ **SCAN PAUSED** due to error:\n`{str(e)}`\n\nUse `/scan` to retry when ready.")

                # Sleep between scans
                await asyncio.sleep(SCAN_INTERVAL_SEC)

        except KeyboardInterrupt:
            logging.info("🛑 Bot stopped by user")
            await self.send("🛑 **Bot stopped manually**")
        except Exception as e:
            logging.error(f"💥 Critical bot error: {e}")
            await self.send(f"💥 **Critical Error:**\n`{str(e)}`\n\n🔄 Bot restarting...")
        finally:
            # Cleanup
            if self.feed:
                await self.feed.close()
            if self.session and not self.session.closed:
                await self.session.close()
            logging.info("🧹 Bot cleanup completed")

# ================== MAIN ENTRY POINT ==================
if __name__ == "__main__":
    try:
        asyncio.run(DivergenceBot().run())
    except Exception as e:
        logging.error(f"Failed to start bot: {e}")
        print(f"❌ Bot startup failed: {e}")

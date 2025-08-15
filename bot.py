# bot.py
# -*- coding: utf-8 -*-
#
# Bot Telegram quét phân kỳ MACD-Histogram (12,26,9)
# - Lệnh: /Scan 4T | /Scan 2t | /Scan 2T | /Scan M5 | /Scan M15 | /Scan M30 | /Scan H1 | /Scan H4 | /Scan D1
# - /Stop để dừng quét
# - Quét 21 cặp mỗi 3 giây, báo 1 lần/nến (không đợi đóng nến)
# - Khung preset:
#   4T = 5m,15m,30m,1h   | 2t = 5m,15m   | 2T = 30m,1h
#
# TODO bạn chỉ cần hoàn thiện DataFeed.get_candles(...) với cTrader Open API (ghi chú bên dưới).

import asyncio
import time
import math
import aiohttp
import logging
from collections import deque, defaultdict
from datetime import datetime, timezone

# ============ CẤU HÌNH BẮT BUỘC CỦA BẠN ============
TELEGRAM_TOKEN = "8358892572:AAHFNZWXwwd_VIL7veQgdLBjNjI253oLCug"
CHAT_ID        = "1676202517"

# Tài khoản cTrader demo (để feed giá từ ICMarkets)
CTRADER_ACCOUNT_ID = "9560393"
CTRADER_PASSWORD   = "phillong00"
CTRADER_SERVER     = "IC Markets Global cTrader Demo"AIRS = [
    "XAUUSD","EURUSD","EURAUD","EURCAD","EURCHF","EURGBP","EURNZD",
    "GBPUSD","GBPAUD","GBPCAD","GBPCHF","GBPNZD","AUDUSD","AUDCAD",
    "AUDCHF","AUDNZD","CADCHF","USDCHF","USDCAD","NZDUSD","NZDCAD","NZDCHF"
]

# Presets khung
PRESETS = {
    "4T":  ["5m","15m","30m","1h"],
    "2t":  ["5m","15m"],
    "2T":  ["30m","1h"],
    "M5":  ["5m"],
    "M15": ["15m"],
    "M30": ["30m"],
    "H1":  ["1h"],
    "H4":  ["4h"],
    "D1":  ["1d"],
}

# Tham số MACD
MACD_FAST   = 12
MACD_SLOW   = 26
MACD_SIGNAL = 9

# Tần suất quét
SCAN_INTERVAL_SEC = 3

# Số nến tối thiểu để tính chỉ báo ổn định
MIN_BARS = 100

# =====================================================

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ---- Chuyển timeframe text -> phút ----
def tf_to_minutes(tf: str) -> int:
    tf = tf.lower()
    if tf.endswith("m"):
        return int(tf[:-1])
    if tf.endsw * 60
    if tf.endswith("d"):
        return int(tf[:-1]) * 60 * 24
    raise ValueError(f"Timeframe không hợp lệ: {tf}")

# ---- EMA tiện ích ----
def ema(series, period):
    if period <= 0 or len(series) == 0:
        return []
    k = 2 / (period + 1)
    out = []
    prev = series[0]
    out.append(prev)
    for i in range(1, len(series)):
        prev = series[i] * k + prev * (1 - k)
        out.append(prev)
    return out

# ---- MACD Histogram ----
def macd_hist(closes, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL):
    if len(closes) < slow + signal + 5:
        return []
    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = ema(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, signal_line)]
    return hist

# ---- Logic phân kỳ theo Pine Script đã chỉnh sửa ----
class DivergenceState:
    # Bearish
    zoneA_hist_pivot = math.nan
    zoneA_p_pivot = math.nan
    in_zoneB = False
    zoneB_occurred = False
    stop_bearish = False

    # Bullish
    zoneA_hist_pivot_bull = math.nan
    zoneA_price_pivot_bull = math.nan
    in_zoneB_bull = False
    zoneB_occurred_bull = False
    stop_bullish = False

    # trạng thái dãy dương/âm
    in_pos_sequence = False
    max_hist_in_sequence = math.nan
    max_price_in_sequence = math.nan

    in_neg_sequence = False
    min_hist_in_sequence = math.nan
    min_price_in_sequence = math.nan

    # kiểm soát báo 1 lần/nến
    last_bar_id_bearish = None
    last_bar_id_bullish = None

    def __init__(self):
        # reset tất cả về nan/False
        self.__dict__.update(DivergenceState().__dict__)

def detect_signals(state: DivergenceState, closes, highs, lows, hist, bar_id):
    """Trả về list các tín hiệu: ["Bearish"] hoặc ["Bullish"] (có thể 0–2) theo đúng logic Pine Script Modified."""
    out = []

    if len(closes) < 3 or len(histeturn out

    h0, h1 = hist[-1], hist[-2]

    # -- Theo dõi dãy dương để tạo Zone A (bearish) --
    pos_sequence_start = (h0 > 0 and h1 <= 0)
    pos_sequence_end   = (h0 <= 0 and h1 > 0)

    if pos_sequence_start:
        state.in_pos_sequence = True
        state.max_hist_in_sequence  = h0
        state.max_price_in_sequence = highs[-1]
    elif state.in_pos_sequence and h0 > 0:
        if h0 > state.max_hist_in_sequence:
            state.max_hist_in_sequence = h0
        if highs[-1] > state.max_price_in_sequence:
            state.max_price_in_sequence = highs[-1]

    if pos_sequence_end and state.in_pos_sequence:
        state.zoneA_hist_pivot  = state.max_hist_in_sequence
        state.zoneA_price_pivot = state.max_price_in_sequence
        state.in_pos_sequence = False
        state.in_zoneB = False
        state.zoneB_occurred = False
        state.stop_bearish = False

    # Zone B (bearish) khi histogram âm một lần sau Zone A
    if not math.isnan(statet) and h0 < 0 and not state.zoneB_occurred:
        state.in_zoneB = True
    if state.in_zoneB and h0 >= 0:
        state.zoneB_occurred = True
        state.in_zoneB = False

    # Stop điều kiện bearish: khi high > zoneA_price_pivot và hist >= zoneA_hist_pivot
    if state.zoneB_occurred and not state.stop_bearish and not math.isnan(state.zoneA_hist_pivot):
        if highs[-1] > state.zoneA_price_pivot and h0 >= state.zoneA_hist_pivot:
            state.stop_bearish = True

    # Bearish signal (đÃ bỏ điều kiện "hist > 0")
    bearish = (not math.isnan(state.zoneA_hist_pivot) and state.zoneB_occurred and
               (not state.stop_bearish) and highs[-1] > state.zoneA_price_pivot and h0 < state.zoneA_hist_pivot)

    if bearish and state.last_bar_id_bearish != bar_id:
        out.append("Bearish")
        state.last_bar_id_bearish = bar_id

    # -- Theo dõi dãy âm để tạo Zone A (bullish) --
    neg_sequence_start = (h0 < 0 and h1 >= 0)
    neg_sequence_end   = (h0 >= 0 and h1 < 0)

    if neg_sequence_start:
        state.in_neg_sequence = True
        state.min_hist_in_sequence  = h0
        state.min_price_in_sequence = lows[-1]
    elif state.in_neg_sequence and h0 < 0:
        if h0 < state.min_hist_in_sequence:
            state.min_hist_in_sequence = h0
        if lows[-1] < state.min_price_in_sequence:
            state.min_price_in_sequence = lows[-1]

    if neg_sequence_end and state.in_neg_sequence:
        state.zoneA_hist_pivot_bull  = state.min_hist_in_sequence
        state.zoneA_price_pivot_bull = state.min_price_in_sequence
        state.in_neg_sequence = False
        state.in_zoneB_bull = False
        state.zoneB_occurred_bull = False
        state.stop_bullish = False

    # Zone B (bullish) khi histogram dương một lần sau Zone A âm
    if not math.isnan(state.zoneA_hist_pivot_bull) and h0 > 0 and not state.zoneB_occurred_bull:
        state.in_zoneB_bull = True
    if state.in_zoneB_bull and h0 <= 0:
        state.zoneB_occurred_bull = True
        state.in_zoneB_bull = False

    # Stop điều kiện bullish: khi low < zoneA_price_pivot_bull và hist <= zoneA_hist_pivot_bull
    if state.zoneB_occurred_bull and not state.stop_bullish and not math.isnan(state.zoneA_hist_pivot_bull):
        if lows[-1] < state.zoneA_price_pivot_bull and h0 <= state.zoneA_hist_pivot_bull:
            state.stop_bullish = True

    # Bullish signal (đÃ bỏ điều kiện "hist < 0")
    bullish = (not math.isnan(state.zoneA_hist_pivot_bull) and state.zoneB_occurred_bull and
               (not state.stop_bullish) and lows[-1] < state.zoneA_price_pivot_bull and h0 > state.zoneA_hist_pivot_bull)

    if bullish and state.last_bar_id_bullish != bar_id:
        out.append("Bullish")
        state.last_bar_id_bullish = bar_id

    return out

# ====== DataFeed: bạn cần điền cTrader Open API tại đây ======
class DataFeed:
    """
    Chuẩn hóa 1 hàm:
 dles(symbol: str, timeframe: str, lookback: int) -> list[dict]
    Trả về danh sách nến gần nhất (mới -> cũ hay cũ -> mới đều được, mình sẽ sắp xếp lại):
      [{'t': unix_sec, 'o':..., 'h':..., 'l':..., 'c':..., 'v':...}, ...]
    """

    def __init__(self):
        # TODO: cTrader Open API setup, OAuth lấy access_token, mở WS/gRPC…
        # Gợi ý nhanh:
        # - Đăng ký app ở https://connect.spotware.com/ lấy CLIENT_ID/SECRET
        # - Đổi lấy access_token grant_type=client_credentials
        # - Dùng Open API stream để subscribe symbol, hoặc gọi history nến
        pass

    async def get_candles(self, symbol: str, timeframe: str, lookback: int):
        """
        TODO: Trả về 'lookback' cây nến của symbol/timeframe từ cTrader ICMarkets.
        TẠM THỜI (để bạn chạy khung bot và Telegram ngay), mình mock bằng Exception.
        Bạn chỉ cần thay phần này bằng gọi cTrader là xong.
        """
        raise Exception("TODO: Kết nối cTrader Open API và trả về dữ liệu nến ở đây.")

# ============ Telegram bot (long-polling) =============
class DivergenceBot:
    def __init__(self):
        self.feed = DataFeed()
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        self.offset = None
        self.running = False
        self.selected_timeframes = []
        self.states = defaultdict(DivergenceState)
        self.history = defaultdict(lambda: {"o": deque(maxlen=1000),
                                            "h": deque(maxlen=1000),
                                            "l": deque(maxlen=1000),
                                            "c": deque(maxlen=1000)})

    async def send(self, text: str):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with self.session.post(url, data={"chat_id": CHAT_ID, "text": text}) as resp:
            if resp.status != 200:
                logging.warning("Send TG failed %s", await resp.text())

    async def fetch_updates(self):
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        params = {"timeout": 50}
        if self.offset:
            params["offset"] = self.offset
        async with self.session.get(url, params=params) as resp:
            data = await resp.json()
            return data.get("result", [])

    async def handle_updates(self):
        updates = await self.fetch_updates()
        for u in updates:
            self.offset = u["update_id"] + 1
            msg = u.get("message") or u.get("edited_message")
            if not msg:
                continue
            chat_id = str(msg["chat"]["id"])
            if chat_id != CHAT_ID:
                # Chỉ nhận lệnh từ CHAT_ID bạn cấu hình
                continue
            text = (msg.get("text") or "").strip()
            if not text:
                continue
            await self.handle_command(text)

    async def handle_command(self, text: str):
        if text.lower().startswith("/stop"):
            self.running = False
            await self.send("Đã dừng quét.")
            return

        if text.lower().startswith("/scan"):
            parts = text.split()
            if len(parts) == 1:
                await self.send("Cú pháp: /Scan <4T|2t|2T|M5|M15|M30|H1|H4|D1>")
                return
            key = parts[1]
            if key not in PRESETS:
                await self.send("Sai preset. Dùng: 4T, 2t, 2T, M5, M15, M30, H1, H4, D1")
                return
            self.selected_timeframes = PRESETS[key]
            self.running = True
            await self.send(f"Bắt đầu quét: {', '.join(self.selected_timeframes)} | Mỗi {SCAN_INTERVAL_SEC}s")

    def bar_id(self, ts_unix: int, timeframe: str) -> int:
        # ID duy nhất cho cây nến: floor(ts / tf_seconds)
        tf_min = tf_to_minutes(timeframe)
        return int(ts_unix // (tf_min * 60))

    async def scan_once(self):
        if not self.running or not self.selected_timeframes:
            return

        for tf in self.selected_timeframes:
            for symbol in PAIRS:
                key = f"{symbol}|{tf}"
                lookback = max(MIN_BARS, MACD_SLOW + MACD_SIGNAL + 50)

                try:
                    candles = await self.feed.get_candles(symbol, tf, lookback)
                except Exception as e:
                    logging.warning("Lỗi lấy nến %s %s: %s", symbol, tf, e)
                    await asyncio.sleep(0.05)
                    continue

                if not candles or len(candles) < MIN_BARS:
                    continue

                # sắp xếp theo thời gian tăng dần
                candles = sorted(candles, key=lambda x: x["t"])

                # đổ dữ liệu vào history
                histo = self.history[key]
                histo["o"].clear(); histo["h"].clear(); histo["l"].clear(); histo["c"].clear()
                for c in candles:
                    histo["o"].append(c["o"])
                    histo["h"].append(c["h"])
                    histo["l"].append(c["l"])
                    histo["c"].append(c["c"])

                closes = list(histo["c"])
                highs  = list(histo["h"])
                lows   = list(histo["l"])

                hist_values = macd_hist(closes)
                if len(hist_values) < 3:
                    continue

                # bar id của cây mới nhất (dùng để anti-spam 1 lần/nến)
                barid = self.bar_id(candles[-1]["t"], tf)

                state = self.states[key]
                sigs = detect_signals(state, closes, highs, lows, hist_values, barid)
                for s in sigs:
                    msg = f"{symbol}, {tf}, {s} Divergence Signal"
                    await self.send(msgnc def run(self):
        # vòng lặp song song: nghe lệnh + quét
        try:
            await self.send("Bot khởi động ✅. Gõ /Scan 4T (hoặc 2t/2T/M5/M15/M30/H1/H4/D1). Gõ /Stop để dừng.")
            while True:
                # chạy 2 task song song: nhận lệnh + quét
                try:
                    await asyncio.wait_for(self.handle_updates(), timeout=1)
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    logging.error("handle_updates error: %s", e)

                try:
                    await self.scan_once()
                except Exception as e:
                    logging.error("scan_once error: %s", e)

                await asyncio.sleep(SCAN_INTERVAL_SEC)
        finally:
            await self.session.close()


if __name__ == "__main__":
    # Chạy event loop
    asyncio.run(DivergenceBot().run()) 

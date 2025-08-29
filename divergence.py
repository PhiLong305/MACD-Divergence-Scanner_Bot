import math

class ClassicDivergenceState:
    def __init__(self):
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

class DivergenceState:
    def __init__(self):
        self.classic = ClassicDivergenceState()
        self.miss_shoulder = MissShoulderDivergenceState()
        self.last_bar_id_bearish_classic = None
        self.last_bar_id_bullish_classic = None
        self.last_bar_id_bearish_ms = None
        self.last_bar_id_bullish_ms = None

def detect_classic_divergence(state: ClassicDivergenceState, closes, highs, lows, hist, bar_index, min_lookbackBars, max_lookbackBars, slopeThreshold):
    signals = {"bearish": False, "bullish": False}
    if len(hist) < 2:
        return signals

    h0 = hist[-1]
    h1 = hist[-2] if len(hist) > 1 else 0
    current_high = highs[-1]
    current_low = lows[-1]

    Classic_pos_sequence_start = h0 >= 0 and h1 < 0
    Classic_pos_sequence_end = h0 < 0 and h1 >= 0

    if Classic_pos_sequence_start:
        state.Classic_max_high_in_sequence = current_high
        state.Classic_max_hist_in_sequence = h0
        state.Classic_bearZoneA_hist_highest_barIndex = bar_index
    elif h0 >= 0:
        if current_high >= state.Classic_max_high_in_sequence:
            state.Classic_max_high_in_sequence = current_high
        if h0 >= state.Classic_max_hist_in_sequence:
            state.Classic_max_hist_in_sequence = h0
            state.Classic_bearZoneA_hist_highest_barIndex = bar_index

    if Classic_pos_sequence_end:
        state.Classic_bearZoneA_high_highest = state.Classic_max_high_in_sequence
        state.Classic_bearZoneA_hist_highest = state.Classic_max_hist_in_sequence
        state.Classic_bearZoneA_bar_highest = state.Classic_bearZoneA_hist_highest_barIndex
        state.Classic_in_bearZoneB = False
        state.Classic_bearZoneB_occurred = False

    if not math.isnan(state.Classic_bearZoneA_hist_highest) and h0 < 0 and not state.Classic_bearZoneB_occurred:
        state.Classic_in_bearZoneB = True

    if state.Classic_in_bearZoneB and h0 >= 0:
        state.Classic_bearZoneB_occurred = True
        state.Classic_in_bearZoneB = False

    bars_since_Classic_bearZoneA_hist_highest = (bar_index - state.Classic_bearZoneA_bar_highest if not math.isnan(state.Classic_bearZoneA_bar_highest) else 0)

    signals["bearish"] = (
        not math.isnan(state.Classic_bearZoneA_hist_highest) and
        state.Classic_bearZoneB_occurred and
        current_high > state.Classic_bearZoneA_high_highest and
        h0 <= slopeThreshold * state.Classic_bearZoneA_hist_highest and
        bars_since_Classic_bearZoneA_hist_highest >= min_lookbackBars and
        bars_since_Classic_bearZoneA_hist_highest <= max_lookbackBars
    )

    if (not math.isnan(state.Classic_bearZoneA_hist_highest) and
        h0 > slopeThreshold * state.Classic_bearZoneA_hist_highest):
        state.Classic_bearZoneA_high_highest = math.nan
        state.Classic_bearZoneA_hist_highest = math.nan
        state.Classic_bearZoneA_hist_highest_barIndex = math.nan
        state.Classic_bearZoneB_occurred = False
        state.Classic_in_bearZoneB = False

    Classic_neg_sequence_start = h0 < 0 and h1 >= 0
    Classic_neg_sequence_end = h0 >= 0 and h1 < 0

    if Classic_neg_sequence_start:
        state.Classic_min_low_in_sequence = current_low
        state.Classic_min_hist_in_sequence = h0
        state.Classic_bullZoneA_hist_lowest_barIndex = bar_index
    elif h0 < 0:
        if current_low <= state.Classic_min_low_in_sequence:
            state.Classic_min_low_in_sequence = current_low
        if h0 <= state.Classic_min_hist_in_sequence:
            state.Classic_min_hist_in_sequence = h0
            state.Classic_bullZoneA_hist_lowest_barIndex = bar_index

    if Classic_neg_sequence_end:
        state.Classic_bullZoneA_low_lowest = state.Classic_min_low_in_sequence
        state.Classic_bullZoneA_hist_lowest = state.Classic_min_hist_in_sequence
        state.Classic_bullZoneA_bar_lowest = state.Classic_bullZoneA_hist_lowest_barIndex
        state.Classic_in_bullZoneB = False
        state.Classic_bullZoneB_occurred = False

    if not math.isnan(state.Classic_bullZoneA_hist_lowest) and h0 >= 0 and not state.Classic_bullZoneB_occurred:
        state.Classic_in_bullZoneB = True

    if state.Classic_in_bullZoneB and h0 <= 0:
        state.Classic_bullZoneB_occurred = True
        state.Classic_in_bullZoneB = False

    bars_since_Classic_bullZoneA_hist_lowest = (bar_index - state.Classic_bullZoneA_bar_lowest if not math.isnan(state.Classic_bullZoneA_bar_lowest) else 0)

    signals["bullish"] = (
        not math.isnan(state.Classic_bullZoneA_hist_lowest) and
        state.Classic_bullZoneB_occurred and
        current_low < state.Classic_bullZoneA_low_lowest and
        h0 >= slopeThreshold * state.Classic_bullZoneA_hist_lowest and
        bars_since_Classic_bullZoneA_hist_lowest >= min_lookbackBars and
        bars_since_Classic_bullZoneA_hist_lowest <= max_lookbackBars
    )

    if (not math.isnan(state.Classic_bullZoneA_hist_lowest) and
        h0 < slopeThreshold * state.Classic_bullZoneA_hist_lowest):
        state.Classic_bullZoneA_low_lowest = math.nan
        state.Classic_bullZoneA_hist_lowest = math.nan
        state.Classic_bullZoneA_bar_lowest = math.nan
        state.Classic_bullZoneB_occurred = False
        state.Classic_in_bullZoneB = False

    return signals

def detect_miss_shoulder_divergence(state: MissShoulderDivergenceState, closes, highs, lows, hist, bar_index, min_lookbackBars, max_lookbackBars):
    signals = {"bearish": False, "bullish": False}
    if len(hist) < 2:
        return signals

    h0 = hist[-1]
    h1 = hist[-2] if len(hist) > 1 else 0
    current_high = highs[-1]
    current_low = lows[-1]

    MS_pos_sequence_start = h0 >= 0 and h1 < 0
    MS_pos_sequence_end = h0 < 0 and h1 >= 0

    if MS_pos_sequence_start:
        state.MS_max_high_in_sequence = current_high
        state.MS_max_hist_in_sequence = h0
        state.MS_bearZoneA_hist_highest_barIndex = bar_index
    elif h0 >= 0:
        if current_high >= state.MS_max_high_in_sequence:
            state.MS_max_high_in_sequence = current_high
        if h0 >= state.MS_max_hist_in_sequence:
            state.MS_max_hist_in_sequence = h0
            state.MS_bearZoneA_hist_highest_barIndex = bar_index

    if MS_pos_sequence_end:
        state.MS_bearZoneA_high_highest = state.MS_max_high_in_sequence
        state.MS_bearZoneA_hist_highest = state.MS_max_hist_in_sequence
        state.MS_bearZoneA_bar_highest = state.MS_bearZoneA_hist_highest_barIndex
        state.MS_in_bearZoneB = False
        state.MS_bearZoneB_occurred = False

    if not math.isnan(state.MS_bearZoneA_hist_highest) and h0 < 0 and not state.MS_bearZoneB_occurred:
        state.MS_in_bearZoneB = True

    if state.MS_in_bearZoneB and h0 > h1:
        state.MS_bearZoneB_occurred = True
        state.MS_in_bearZoneB = True

    bars_since_MS_bearZoneA_hist_highest = (bar_index - state.MS_bearZoneA_bar_highest if not math.isnan(state.MS_bearZoneA_bar_highest) else 0)

    signals["bearish"] = (
        not math.isnan(state.MS_bearZoneA_hist_highest) and
        state.MS_bearZoneB_occurred and
        current_high > state.MS_bearZoneA_high_highest and
        h0 < 0 and
        h0 > h1 and
        bars_since_MS_bearZoneA_hist_highest >= min_lookbackBars and
        bars_since_MS_bearZoneA_hist_highest <= max_lookbackBars
    )

    MS_neg_sequence_start = h0 < 0 and h1 >= 0
    MS_neg_sequence_end = h0 >= 0 and h1 < 0

    if MS_neg_sequence_start:
        state.MS_min_low_in_sequence = current_low
        state.MS_min_hist_in_sequence = h0
        state.MS_bullZoneA_hist_lowest_barIndex = bar_index
    elif h0 < 0:
        if current_low <= state.MS_min_low_in_sequence:
            state.MS_min_low_in_sequence = current_low
        if h0 <= state.MS_min_hist_in_sequence:
            state.MS_min_hist_in_sequence = h0
            state.MS_bullZoneA_hist_lowest_barIndex = bar_index

    if MS_neg_sequence_end:
        state.MS_bullZoneA_low_lowest = state.MS_min_low_in_sequence
        state.MS_bullZoneA_hist_lowest = state.MS_min_hist_in_sequence
        state.MS_bullZoneA_bar_lowest = state.MS_bullZoneA_hist_lowest_barIndex
        state.MS_in_bullZoneB = False
        state.MS_bullZoneB_occurred = False

    if (not math.isnan(state.MS_bullZoneA_hist_lowest) and
        h0 >= 0 and
        not state.MS_bullZoneB_occurred):
        state.MS_in_bullZoneB = True

    if state.MS_in_bullZoneB and h0 < h1:
        state.MS_bullZoneB_occurred = True
        state.MS_in_bullZoneB = True

    bars_since_MS_bullZoneA_hist_lowest = (bar_index - state.MS_bullZoneA_bar_lowest if not math.isnan(state.MS_bullZoneA_bar_lowest) else 0)

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

    bar_index = len(closes) - 1

    classic_signals = detect_classic_divergence(
        state.classic,
        closes, highs, lows, hist,
        bar_index, 10, 40, 0.7
    )

    miss_shoulder_signals = detect_miss_shoulder_divergence(
        state.miss_shoulder,
        closes, highs, lows, hist,
        bar_index, 10, 40
    )

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

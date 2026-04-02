"""
Institutional efficiency standard (1W = 4W): **identical** math for
`signal_tracker` weekly audit (Mon–Fri window) and `system_performance_ledger` 4W horizon.

Both use `compute_directional_excursions` → WIN iff (final return in bias direction > 0) OR
(MFE% > ADJUSTED_WIN_MFE_THRESHOLD_PCT). No close-only / binary rule.
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

ADJUSTED_WIN_MFE_THRESHOLD_PCT = 2.0
MAE_ER_FLOOR = 0.001


def compute_directional_excursions(
    p_entry: float,
    p_min: float,
    p_max: float,
    p_close: float,
    direction: str,
) -> Tuple[float, float, float, float, Optional[int]]:
    """
    Primary outcome: **efficiency_win** (1/0) = terminal return in bias direction > 0 OR
    MFE% > ADJUSTED_WIN_MFE_THRESHOLD_PCT (volatility-aware; not close-only).

    Returns: return_pct, mfe_pct, mae_pct, efficiency_ratio, efficiency_win.
    NEUTRAL direction → Nones for the win flag.
    """
    if p_entry <= 0 or p_min <= 0 or p_max <= 0:
        return 0.0, 0.0, 0.0, 0.0, None
    d = str(direction or "").upper()
    if d == "LONG":
        ret = (p_close - p_entry) / p_entry * 100.0
        mfe = (p_max - p_entry) / p_entry * 100.0
        mae = (p_entry - p_min) / p_entry * 100.0
        mfe_c = max(0.0, mfe)
        mae_c = max(0.0, mae)
        er = mfe_c / max(mae_c, MAE_ER_FLOOR)
        win = 1 if (ret > 0.0 or mfe_c > ADJUSTED_WIN_MFE_THRESHOLD_PCT) else 0
        return ret, mfe_c, mae_c, float(er), win
    if d == "SHORT":
        ret = (p_entry - p_close) / p_entry * 100.0
        mfe = (p_entry - p_min) / p_entry * 100.0
        mae = (p_max - p_entry) / p_entry * 100.0
        mfe_c = max(0.0, mfe)
        mae_c = max(0.0, mae)
        er = mfe_c / max(mae_c, MAE_ER_FLOOR)
        win = 1 if (ret > 0.0 or mfe_c > ADJUSTED_WIN_MFE_THRESHOLD_PCT) else 0
        return ret, mfe_c, mae_c, float(er), win
    return 0.0, 0.0, 0.0, 0.0, None


def bias_to_direction(bias: str) -> str:
    s = str(bias or "").upper()
    if any(x in s for x in ("LONG", "BUY", "BULLISH")):
        return "LONG"
    if any(x in s for x in ("SHORT", "SELL", "BEARISH")):
        return "SHORT"
    return "NEUTRAL"

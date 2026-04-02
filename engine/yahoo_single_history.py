"""
Single-symbol Yahoo data via Ticker.history.

Prefer this over yf.download(ticker) for one ticker: avoids MultiIndex column bugs
that broke spot prices (e.g. DXY) in some yfinance versions / environments.
"""
from __future__ import annotations

from typing import Any

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

from .retry_util import run_with_retries


def _strip_tz_index(obj: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    idx = obj.index
    if getattr(idx, "tz", None) is not None:
        out = obj.copy()
        out.index = pd.DatetimeIndex(idx).tz_localize(None)
        return out
    return obj


def history_close_series(
    ticker: str,
    *,
    start=None,
    end=None,
    period: str | None = None,
    interval: str = "1d",
    auto_adjust: bool = True,
) -> pd.Series:
    """
    Daily (or other interval) adjusted close for one symbol.
    Use either (period) or (start, end), not both required — same rules as yfinance.
    """
    if yf is None or not str(ticker or "").strip():
        return pd.Series(dtype=float)
    sym = str(ticker).strip()

    def _pull():
        kw: dict[str, Any] = {"interval": interval, "auto_adjust": auto_adjust}
        if period:
            kw["period"] = period
        else:
            kw["start"] = start
            kw["end"] = end
        return yf.Ticker(sym).history(**kw)

    try:
        hist = run_with_retries(
            _pull,
            attempts=4,
            base_seconds=1.5,
            max_sleep=45.0,
            exponential=True,
            default=None,
        )
    except Exception:
        return pd.Series(dtype=float)
    if hist is None or hist.empty or "Close" not in hist.columns:
        return pd.Series(dtype=float)
    s = pd.to_numeric(hist["Close"], errors="coerce").dropna()
    if s.empty:
        return pd.Series(dtype=float)
    s = _strip_tz_index(s)
    return s.sort_index()


def history_ohlc_dataframe(
    ticker: str,
    *,
    start=None,
    end=None,
    period: str | None = None,
    interval: str = "1d",
    auto_adjust: bool = True,
) -> pd.DataFrame:
    """OHLCV bars for swing / ATR logic; empty if unavailable."""
    if yf is None or not str(ticker or "").strip():
        return pd.DataFrame()
    sym = str(ticker).strip()

    def _pull():
        kw: dict[str, Any] = {"interval": interval, "auto_adjust": auto_adjust}
        if period:
            kw["period"] = period
        else:
            kw["start"] = start
            kw["end"] = end
        return yf.Ticker(sym).history(**kw)

    try:
        hist = run_with_retries(
            _pull,
            attempts=4,
            base_seconds=1.5,
            max_sleep=45.0,
            exponential=True,
            default=None,
        )
    except Exception:
        return pd.DataFrame()
    if hist is None or hist.empty:
        return pd.DataFrame()
    need = ("Open", "High", "Low", "Close")
    if not all(c in hist.columns for c in need):
        return pd.DataFrame()
    cols = list(need)
    if "Volume" in hist.columns:
        cols.append("Volume")
    out = hist[cols].copy()
    out = _strip_tz_index(out)
    return out.dropna(how="all").sort_index()

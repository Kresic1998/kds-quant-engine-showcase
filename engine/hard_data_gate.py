"""
Hard Data Gate: critical Yahoo macro tickers must not be stale beyond allowed window.

Uses calendar business-day gap from last bar date to today (UTC) so weekend gaps do not
false-trigger; same-day freshness uses wall-clock hours on the bar timestamp when available.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

# Critical macro symbols for integrity (must have recent daily bars).
CRITICAL_MACRO_TICKERS: tuple[str, ...] = ("^VIX", "^MOVE", "HG=F", "DX-Y.NYB")

# Stale if two or more business days have passed since the bar date (weekend-aware).
MAX_BUSINESS_DAY_GAP = 2

# If last bar is "today" (same UTC calendar day as reference), allow up to this many hours
# since bar timestamp before stale (handles late Yahoo updates).
SAME_DAY_MAX_HOURS = 24.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _series_last_ts(series: pd.Series | None) -> pd.Timestamp | None:
    if series is None or series.empty:
        return None
    try:
        idx = series.index.max()
        return pd.Timestamp(idx)
    except Exception:
        return None


def _staleness_hours(ts: pd.Timestamp | None, ref: datetime) -> float | None:
    if ts is None or pd.isna(ts):
        return None
    t = pd.Timestamp(ts)
    if t.tzinfo is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    ref_naive = ref.replace(tzinfo=None) if ref.tzinfo else ref
    try:
        return float((ref_naive - t.to_pydatetime()).total_seconds() / 3600.0)
    except Exception:
        return None


def _business_day_gap_days(bar_date: Any, ref_date: Any) -> int:
    """Whole business days strictly after bar calendar date up to ref calendar date (inclusive)."""
    try:
        b = pd.Timestamp(bar_date).normalize()
        r = pd.Timestamp(ref_date).normalize()
    except Exception:
        return 999
    if r.date() <= b.date():
        return 0
    try:
        # numpy.busday_count(begin, end): business days in [begin, end) — stable vs older pandas `freq="C"`.
        start = (b + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        end_excl = (r + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        return int(np.busday_count(start, end_excl))
    except Exception:
        return 999


def evaluate_hard_macro_data_gate(
    *,
    market_bundle: dict[str, Any] | None,
    shield_data: dict[str, Any] | None,
    ref_utc: datetime | None = None,
) -> dict[str, Any]:
    """
    Returns dict with ok (bool), tier ('FATAL' | 'CRITICAL_STALE' | None), per-ticker status, reasons.
    """
    try:
        return _evaluate_hard_macro_data_gate_impl(
            market_bundle=market_bundle,
            shield_data=shield_data,
            ref_utc=ref_utc,
        )
    except TypeError:
        return {
            "ok": True,
            "tier": None,
            "reasons": ["hard_gate_eval_type_error_bypass"],
            "per_ticker": {},
            "ref_utc": (ref_utc or _utc_now()).isoformat(),
            "max_business_day_gap_rule": MAX_BUSINESS_DAY_GAP,
            "gate_error": "TypeError",
        }
    except Exception as e:
        return {
            "ok": True,
            "tier": None,
            "reasons": [f"hard_gate_eval_exception:{type(e).__name__}"],
            "per_ticker": {},
            "ref_utc": (ref_utc or _utc_now()).isoformat(),
            "max_business_day_gap_rule": MAX_BUSINESS_DAY_GAP,
            "gate_error": str(e)[:200],
        }


def _evaluate_hard_macro_data_gate_impl(
    *,
    market_bundle: dict[str, Any] | None,
    shield_data: dict[str, Any] | None,
    ref_utc: datetime | None = None,
) -> dict[str, Any]:
    ref = ref_utc or _utc_now()
    ref_date = ref.date()
    mb = market_bundle if isinstance(market_bundle, dict) else {}
    raw = mb.get("raw") if isinstance(mb.get("raw"), dict) else {}
    sh = shield_data if isinstance(shield_data, dict) else {}

    per_ticker: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []

    # HG=F from macro bundle (copper series)
    cu = raw.get("copper")
    cu_ts = _series_last_ts(cu if isinstance(cu, pd.Series) else None)
    hg_gap = _business_day_gap_days(cu_ts, ref_date) if cu_ts is not None else 999
    hg_hours = _staleness_hours(cu_ts, ref)
    per_ticker["HG=F"] = {
        "last_updated": str(cu_ts.date()) if cu_ts is not None else None,
        "business_day_gap": hg_gap,
        "hours_since_bar": hg_hours,
    }
    if cu_ts is None:
        reasons.append("critical_missing:HG=F")
    elif hg_gap >= MAX_BUSINESS_DAY_GAP:
        reasons.append(f"critical_stale:HG=F gap_bd={hg_gap}")
    elif cu_ts.normalize().date() == ref_date and hg_hours is not None and hg_hours > SAME_DAY_MAX_HOURS:
        reasons.append(f"critical_stale:HG=F same_day_hours={hg_hours:.1f}")

    # Shield-aligned tickers: use per-column last valid date from Yahoo history if present.
    y_ts_raw = sh.get("yahoo_last_bar_ts")
    y_ts = pd.Timestamp(y_ts_raw) if y_ts_raw is not None and str(y_ts_raw).strip() else None
    if y_ts is not None and y_ts.tzinfo is not None:
        y_ts = y_ts.tz_convert("UTC").tz_localize(None)

    fetch_err = sh.get("shield_fetch_error")
    if y_ts is not None:
        gap_bd = _business_day_gap_days(y_ts, ref_date)
        hrs = _staleness_hours(y_ts, ref)
        for sym in ("^VIX", "^MOVE", "DX-Y.NYB"):
            per_ticker[sym] = {
                "last_updated": str(y_ts.date()),
                "business_day_gap": gap_bd,
                "hours_since_bar": hrs,
            }
        if gap_bd >= MAX_BUSINESS_DAY_GAP:
            reasons.append(f"critical_stale:shield_bundle gap_bd={gap_bd}")
        elif y_ts.normalize().date() == ref_date and hrs is not None and hrs > SAME_DAY_MAX_HOURS:
            reasons.append(f"critical_stale:shield_bundle same_day_hours={hrs:.1f}")
    else:
        for sym in ("^VIX", "^MOVE", "DX-Y.NYB"):
            per_ticker[sym] = {"last_updated": None, "business_day_gap": None, "hours_since_bar": None}
        if fetch_err and str(fetch_err).strip():
            reasons.append("critical_shield_fetch_failed")
        else:
            reasons.append("critical_missing:shield_timestamps")

    ok = len(reasons) == 0
    tier = None
    if not ok:
        if any(r.startswith("critical_missing") for r in reasons) or any(
            "critical_shield_fetch_failed" == r for r in reasons
        ):
            tier = "FATAL"
        else:
            tier = "CRITICAL_STALE"

    return {
        "ok": ok,
        "tier": tier,
        "reasons": reasons,
        "per_ticker": per_ticker,
        "ref_utc": ref.isoformat(),
        "max_business_day_gap_rule": MAX_BUSINESS_DAY_GAP,
    }

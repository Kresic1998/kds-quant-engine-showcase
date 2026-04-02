"""
System performance ledger: 4W MAE/MFE, calibration buckets, shield split.
Populated when Quant AI logs a signal; prices filled by refresh_performance_ledger_prices().
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

import pandas as pd
import yfinance as yf

from .audit_engine import _resolve_ticker_for_audit
from .audit_metrics import compute_directional_excursions
from .config import DB_NAME, DISPLAY_MAP, TICKER_MAP  # DB_NAME: tests monkeypatch
from .db_backend import get_connection, read_sql_pandas, use_postgresql
from .retry_http import yfinance_download_retry


def _audit_lookup_map() -> dict:
    lookup_map: dict = {}
    for full_name, ticker in TICKER_MAP.items():
        short_code = DISPLAY_MAP.get(full_name, "")
        lookup_map[full_name] = ticker
        lookup_map[str(full_name).upper()] = ticker
        if short_code:
            lookup_map[short_code] = ticker
            lookup_map[str(short_code).upper()] = ticker
    # Alijasi kao u Macro Swing watchlistu (nisu u DISPLAY_MAP kao ključevi).
    lookup_map.setdefault("SILVER", "SI=F")
    lookup_map.setdefault("GOLD", "GC=F")
    lookup_map.setdefault("USOIL", "CL=F")
    return lookup_map


def get_ledger_audit_lookup_map() -> dict:
    """Javni alias za Yahoo ticker rezoluciju (ledger / dual-horizon)."""
    return _audit_lookup_map()


def _bias_direction(bias: str) -> str:
    s = str(bias or "").upper()
    if any(x in s for x in ("LONG", "BUY", "BULLISH", "STRONG LONG")):
        return "LONG"
    if any(x in s for x in ("SHORT", "SELL", "BEARISH", "STRONG SHORT")):
        return "SHORT"
    return "NEUTRAL"


def _score_bucket(score: float) -> str:
    x = float(score or 0.0)
    edges = [0, 2, 4, 6, 8, 10]
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        if i == len(edges) - 2:
            if lo <= x <= hi:
                return f"[{lo}-{hi}]"
        else:
            if lo <= x < hi:
                return f"[{lo}-{hi})"
    return "[0-10]"


def _compute_excursions(
    p_entry: float, p_min: float, p_max: float, p_close: float, direction: str
) -> Tuple[float, float, float, float, Optional[int], Optional[int]]:
    """
    4W ledger excursions — **same** `audit_metrics.compute_directional_excursions` as 1W `run_auto_audit`
    (ADJUSTED_WIN_MFE_THRESHOLD_PCT, MFE/MAE/ER). directional_win == adjusted_win (efficiency int).
    """
    ret, mfe, mae, er, win = compute_directional_excursions(
        p_entry, p_min, p_max, p_close, direction
    )
    if direction == "NEUTRAL":
        return 0.0, 0.0, 0.0, 0.0, None, None
    return ret, mfe, mae, er, win, win


def refresh_performance_ledger_prices(*, max_rows: int = 200) -> tuple[int, str]:
    """
    For PENDING rows with horizon_end_4w <= today, fetch Yahoo daily OHLC and fill MAE/MFE.
    """
    lookup = _audit_lookup_map()
    now = datetime.now(timezone.utc).date()
    filled = 0
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, instrument, predicted_bias, p_entry, horizon_end_4w, created_at
                FROM system_performance_ledger
                WHERE status = 'PENDING' AND date(horizon_end_4w) <= date(?)
                ORDER BY id ASC
                LIMIT ?
                """,
                (now.isoformat(), int(max_rows)),
            ).fetchall()
            for rid, inst, bias, p_entry, horizon_end, created_at in rows:
                ticker = _resolve_ticker_for_audit(inst, lookup)
                if not ticker:
                    conn.execute(
                        "UPDATE system_performance_ledger SET status = 'NO_DATA' WHERE id = ?",
                        (int(rid),),
                    )
                    continue
                try:
                    t0 = pd.to_datetime(created_at, errors="coerce")
                    if pd.isna(t0):
                        t0 = pd.Timestamp.now(tz="UTC")
                    if getattr(t0, "tzinfo", None) is not None:
                        t0 = t0.tz_convert(None)
                    start = (t0.normalize() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    end = (pd.Timestamp(horizon_end) + pd.Timedelta(days=2)).strftime("%Y-%m-%d")
                    df = yfinance_download_retry(
                        lambda: yf.download(
                            ticker, start=start, end=end, interval="1d", progress=False, auto_adjust=True
                        ),
                        attempts=4,
                        base_seconds=2.0,
                        default=pd.DataFrame(),
                    )
                    if df.empty or "High" not in df.columns:
                        conn.execute(
                            "UPDATE system_performance_ledger SET status = 'NO_DATA' WHERE id = ?",
                            (int(rid),),
                        )
                        continue
                    if isinstance(df.columns, pd.MultiIndex):
                        hi = df["High"][ticker].dropna()
                        lo = df["Low"][ticker].dropna()
                        cl = df["Close"][ticker].dropna()
                    else:
                        hi = df["High"].dropna()
                        lo = df["Low"].dropna()
                        cl = df["Close"].dropna()
                    if hi.empty or lo.empty or cl.empty:
                        conn.execute(
                            "UPDATE system_performance_ledger SET status = 'NO_DATA' WHERE id = ?",
                            (int(rid),),
                        )
                        continue
                    t0d = t0.normalize()
                    he = pd.Timestamp(horizon_end).normalize()
                    mask = (hi.index >= t0d) & (hi.index <= he)
                    hi_w = hi.loc[mask]
                    lo_w = lo.loc[mask]
                    cl_w = cl.loc[mask]
                    if hi_w.empty:
                        conn.execute(
                            "UPDATE system_performance_ledger SET status = 'NO_DATA' WHERE id = ?",
                            (int(rid),),
                        )
                        continue
                    p_max = float(hi_w.max())
                    p_min = float(lo_w.min())
                    p_close = float(cl_w.iloc[-1])
                    pe_raw = p_entry
                    try:
                        pe = float(pe_raw) if pe_raw is not None and pe_raw == pe_raw and float(pe_raw) > 0 else None
                    except (TypeError, ValueError):
                        pe = None
                    if pe is None:
                        pe = float(cl.loc[cl.index >= t0d].iloc[0]) if not cl.loc[cl.index >= t0d].empty else float(cl.iloc[0])
                    direction = _bias_direction(bias)
                    ret, mfe, mae, er, dwin, adjw = _compute_excursions(pe, p_min, p_max, p_close, direction)
                    conn.execute(
                        """
                        UPDATE system_performance_ledger SET
                          p_max_4w = ?, p_min_4w = ?, p_close_4w = ?,
                          return_4w_pct = ?, mfe_pct = ?, mae_pct = ?,
                          efficiency_ratio = ?, adjusted_win = ?,
                          directional_win = ?, status = 'FILLED'
                        WHERE id = ?
                        """,
                        (
                            p_max,
                            p_min,
                            p_close,
                            ret,
                            mfe,
                            mae,
                            er,
                            adjw,
                            dwin,
                            int(rid),
                        ),
                    )
                    filled += 1
                except Exception:
                    conn.execute(
                        "UPDATE system_performance_ledger SET status = 'NO_DATA' WHERE id = ?",
                        (int(rid),),
                    )
            conn.commit()
        return filled, f"Filled {filled} ledger row(s)."
    except Exception as e:
        return 0, str(e)


def refetch_filled_ledger_metrics(*, max_rows: int = 200) -> tuple[int, str]:
    """
    Recompute MFE/MAE/ER/adjusted_win/directional_win for existing FILLED rows (e.g. after logic upgrade).
    """
    lookup = _audit_lookup_map()
    updated = 0
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, instrument, predicted_bias, p_entry, horizon_end_4w, created_at
                FROM system_performance_ledger
                WHERE status = 'FILLED'
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(max_rows),),
            ).fetchall()
            for rid, inst, bias, p_entry, horizon_end, created_at in rows:
                ticker = _resolve_ticker_for_audit(inst, lookup)
                if not ticker:
                    continue
                try:
                    t0 = pd.to_datetime(created_at, errors="coerce")
                    if pd.isna(t0):
                        continue
                    if getattr(t0, "tzinfo", None) is not None:
                        t0 = t0.tz_convert(None)
                    start = (t0.normalize() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    end = (pd.Timestamp(horizon_end) + pd.Timedelta(days=2)).strftime("%Y-%m-%d")
                    df = yfinance_download_retry(
                        lambda: yf.download(
                            ticker, start=start, end=end, interval="1d", progress=False, auto_adjust=True
                        ),
                        attempts=4,
                        base_seconds=2.0,
                        default=pd.DataFrame(),
                    )
                    if df.empty or "High" not in df.columns:
                        continue
                    if isinstance(df.columns, pd.MultiIndex):
                        hi = df["High"][ticker].dropna()
                        lo = df["Low"][ticker].dropna()
                        cl = df["Close"][ticker].dropna()
                    else:
                        hi = df["High"].dropna()
                        lo = df["Low"].dropna()
                        cl = df["Close"].dropna()
                    if hi.empty or lo.empty or cl.empty:
                        continue
                    t0d = t0.normalize()
                    he = pd.Timestamp(horizon_end).normalize()
                    mask = (hi.index >= t0d) & (hi.index <= he)
                    hi_w = hi.loc[mask]
                    lo_w = lo.loc[mask]
                    cl_w = cl.loc[mask]
                    if hi_w.empty:
                        continue
                    p_max = float(hi_w.max())
                    p_min = float(lo_w.min())
                    p_close = float(cl_w.iloc[-1])
                    pe_raw = p_entry
                    try:
                        pe = float(pe_raw) if pe_raw is not None and pe_raw == pe_raw and float(pe_raw) > 0 else None
                    except (TypeError, ValueError):
                        pe = None
                    if pe is None:
                        pe = float(cl.loc[cl.index >= t0d].iloc[0]) if not cl.loc[cl.index >= t0d].empty else float(cl.iloc[0])
                    direction = _bias_direction(bias)
                    ret, mfe, mae, er, dwin, adjw = _compute_excursions(pe, p_min, p_max, p_close, direction)
                    conn.execute(
                        """
                        UPDATE system_performance_ledger SET
                          p_max_4w = ?, p_min_4w = ?, p_close_4w = ?,
                          return_4w_pct = ?, mfe_pct = ?, mae_pct = ?,
                          efficiency_ratio = ?, adjusted_win = ?,
                          directional_win = ?
                        WHERE id = ?
                        """,
                        (p_max, p_min, p_close, ret, mfe, mae, er, adjw, dwin, int(rid)),
                    )
                    updated += 1
                except Exception:
                    continue
            conn.commit()
        return updated, f"Refetched metrics for {updated} FILLED ledger row(s)."
    except Exception as e:
        return 0, str(e)


def load_filled_ledger_df() -> pd.DataFrame:
    try:
        return read_sql_pandas(
            "SELECT * FROM system_performance_ledger WHERE status = 'FILLED' ORDER BY id DESC"
        )
    except Exception:
        return pd.DataFrame()


def load_single_signals_ledger_df(*, limit_rows: int = 2000) -> pd.DataFrame:
    """Single-instrument ledger rows (Quant AI singles) for dual-horizon price checks."""
    try:
        if use_postgresql():
            q = """
                SELECT * FROM system_performance_ledger
                WHERE signal_type IS NULL OR TRIM(LOWER(signal_type)) = 'single'
                ORDER BY created_at DESC NULLS LAST
                LIMIT ?
                """
        else:
            q = """
                SELECT * FROM system_performance_ledger
                WHERE signal_type IS NULL OR TRIM(LOWER(signal_type)) = 'single'
                ORDER BY datetime(created_at) DESC
                LIMIT ?
                """
        return read_sql_pandas(q, (int(limit_rows),))
    except Exception:
        return pd.DataFrame()


def ledger_status_counts() -> dict[str, Any]:
    """Counts by status + backfill rows (for UI diagnostics on Streamlit Cloud vs local DB)."""
    out: dict[str, Any] = {"by_status": {}, "backfill_rows": 0, "table_exists": False, "error": None}
    try:
        with get_connection() as conn:
            if use_postgresql():
                cur = conn.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM information_schema.tables
                      WHERE table_schema = 'public' AND table_name = 'system_performance_ledger'
                    )
                    """
                )
                if not (cur.fetchone() or [False])[0]:
                    return out
            else:
                cur = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='system_performance_ledger'"
                )
                if not cur.fetchone():
                    return out
            out["table_exists"] = True
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM system_performance_ledger GROUP BY status"
            ).fetchall()
            out["by_status"] = {str(s or ""): int(c) for s, c in rows}
            if use_postgresql():
                cur2 = conn.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'system_performance_ledger'
                    """
                )
                colset = {r[0] for r in cur2.fetchall()}
            else:
                colset = {r[1] for r in conn.execute("PRAGMA table_info(system_performance_ledger)").fetchall()}
            if "backfill_source" in colset:
                bf = conn.execute(
                    "SELECT COUNT(*) FROM system_performance_ledger WHERE backfill_source IS NOT NULL AND backfill_source != ''"
                ).fetchone()
                out["backfill_rows"] = int(bf[0]) if bf else 0
            else:
                out["backfill_rows"] = 0
    except Exception as e:
        out["error"] = str(e)
    return out


def _parse_snap_cell(raw: Any) -> dict[str, Any]:
    try:
        return json.loads(raw or "{}")
    except Exception:
        return {}


def enrich_ledger_with_regime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add shock_type, curve_signal, shield_snap, dxy_trend_snap, scenario_snap from regime_snapshot JSON."""
    if df is None or df.empty or "regime_snapshot" not in df.columns:
        return df.copy() if df is not None else pd.DataFrame()
    out = df.copy()
    snaps = out["regime_snapshot"].apply(_parse_snap_cell)
    out["shock_type"] = snaps.apply(lambda s: str(s.get("shock_type") or ""))
    out["curve_signal"] = snaps.apply(lambda s: str(s.get("curve_signal") or ""))
    out["shield_snap"] = snaps.apply(lambda s: bool(s.get("shield_active")))
    out["dxy_trend_snap"] = snaps.apply(lambda s: str(s.get("dxy_trend") or "NEUTRAL").upper())
    out["scenario_snap"] = snaps.apply(lambda s: str(s.get("scenario") or ""))
    return out


def filter_filled_ledger_for_explorer(
    df: pd.DataFrame,
    *,
    weeks_back: Optional[int] = None,
    instrument: Optional[str] = None,
    shock_types: Optional[list] = None,
    curve_signals: Optional[list] = None,
    shield_active: Optional[bool] = None,
    dxy_trend: Optional[str] = None,
    scenarios: Optional[list] = None,
) -> pd.DataFrame:
    """
    Subset FILLED ledger rows for UI exploration.
    weeks_back: keep rows whose created_at is within last N*7 days (approximate weeks).
    instrument: exact match on instrument column (e.g. EUR, NQ1!).
    """
    if df is None or df.empty:
        return pd.DataFrame()
    d = enrich_ledger_with_regime_columns(df)
    if weeks_back is not None and int(weeks_back) > 0 and "created_at" in d.columns:
        cutoff = datetime.now(timezone.utc) - timedelta(days=7 * int(weeks_back))
        ts = pd.to_datetime(d["created_at"], errors="coerce", utc=True)
        d = d[ts >= pd.Timestamp(cutoff)]
    if instrument and str(instrument).strip() and str(instrument).strip().upper() != "(ALL)":
        d = d[d["instrument"].astype(str) == str(instrument).strip()]
    if shock_types:
        want = {str(x) for x in shock_types if str(x).strip()}
        if want:
            d = d[d["shock_type"].isin(want)]
    if curve_signals:
        want = {str(x) for x in curve_signals if str(x).strip()}
        if want:
            d = d[d["curve_signal"].isin(want)]
    if shield_active is not None:
        d = d[d["shield_snap"] == bool(shield_active)]
    if dxy_trend and str(dxy_trend).strip() and str(dxy_trend).strip().upper() not in ("(ANY)", "ANY", ""):
        d = d[d["dxy_trend_snap"] == str(dxy_trend).strip().upper()]
    if scenarios:
        want = {str(x) for x in scenarios if str(x).strip()}
        if want:
            d = d[d["scenario_snap"].isin(want)]
    return d


def ledger_win_rate_diagnostics(df: pd.DataFrame) -> dict[str, Any]:
    """Debug: why win rate may look unchanged (null wins, ER coverage)."""
    if df is None or df.empty:
        return {"rows": 0}
    out: dict[str, Any] = {"rows": int(len(df))}
    if "directional_win" in df.columns:
        s = df["directional_win"]
        out["directional_win_non_null"] = int(s.notna().sum())
        out["directional_win_null"] = int(s.isna().sum())
        sn = s.dropna()
        if len(sn) > 0:
            out["directional_win_mean"] = float(sn.mean())
    if "efficiency_ratio" in df.columns:
        er = pd.to_numeric(df["efficiency_ratio"], errors="coerce")
        out["efficiency_ratio_non_null"] = int(er.notna().sum())
    return out


def win_rate_summary_for_df(df: pd.DataFrame) -> dict[str, Any]:
    """Aggregate for filtered FILLED ledger: `directional_win` = efficiency win (MFE/return rule)."""
    if df is None or df.empty or "directional_win" not in df.columns:
        return {"n": 0}
    s = df["directional_win"].dropna()
    n = int(s.size)
    if n == 0:
        return {"n": 0}
    out: dict[str, Any] = {
        "n": n,
        "win_rate_pct": float(s.mean() * 100.0),
        "avg_mae_pct": float(df["mae_pct"].mean()) if "mae_pct" in df.columns and df["mae_pct"].notna().any() else None,
        "avg_mfe_pct": float(df["mfe_pct"].mean()) if "mfe_pct" in df.columns and df["mfe_pct"].notna().any() else None,
    }
    if "efficiency_ratio" in df.columns and df["efficiency_ratio"].notna().any():
        out["avg_efficiency_ratio"] = float(df["efficiency_ratio"].dropna().mean())
    return out


def win_rate_by_instrument(df: pd.DataFrame) -> pd.DataFrame:
    """Per-instrument stats for FILLED ledger (after caller applies filters)."""
    if df is None or df.empty or "instrument" not in df.columns or "directional_win" not in df.columns:
        return pd.DataFrame()
    rows = []
    for inst, g in df.groupby(df["instrument"].astype(str), dropna=False):
        summ = win_rate_summary_for_df(g)
        if summ.get("n", 0) > 0:
            row = {
                "instrument": inst,
                "avg_efficiency_ratio": summ.get("avg_efficiency_ratio"),
                "n": summ["n"],
                "win_rate_pct": summ["win_rate_pct"],
                "avg_mae_pct": summ.get("avg_mae_pct"),
                "avg_mfe_pct": summ.get("avg_mfe_pct"),
            }
            rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("n", ascending=False).reset_index(drop=True)


def calibration_by_score_bucket(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "master_score" not in df.columns:
        return pd.DataFrame()
    d = df.copy()
    d["bucket"] = d["master_score"].apply(_score_bucket)
    g = d.groupby("bucket", dropna=False)
    out = g.agg(
        n=("id", "count"),
        win_rate=(
            "directional_win",
            lambda s: float(s.dropna().mean() * 100.0) if s.dropna().size > 0 else None,
        ),
        avg_mfe=("mfe_pct", "mean"),
        avg_mae=("mae_pct", "mean"),
        avg_er=("efficiency_ratio", "mean"),
    ).reset_index()
    return out


def shield_efficacy_split(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    rows = []
    for _, r in df.iterrows():
        try:
            snap = json.loads(r.get("regime_snapshot") or "{}")
        except Exception:
            snap = {}
        active = bool(snap.get("shield_active"))
        rows.append({**r.to_dict(), "shield_active": active})
    d = pd.DataFrame(rows)
    if d.empty or "directional_win" not in d.columns:
        return pd.DataFrame()
    parts = []
    for label, sub in [("shield_on", d[d["shield_active"] == True]), ("shield_off", d[d["shield_active"] == False])]:
        if sub.empty:
            continue
        wr = float(sub["directional_win"].mean() * 100.0) if sub["directional_win"].notna().any() else None
        parts.append(
            {
                "segment": label,
                "n": len(sub),
                "win_rate_pct": wr,
                "avg_mfe_pct": float(sub["mfe_pct"].mean()) if "mfe_pct" in sub.columns else None,
            }
        )
    return pd.DataFrame(parts)


def setup_match_stats(
    df: pd.DataFrame,
    *,
    shock_type: Optional[str],
    curve_signal: Optional[str],
    shield_active: Optional[bool],
    dxy_trend: Optional[str] = None,
    scenario_in: Optional[list] = None,
) -> dict[str, Any]:
    """Aggregate stats for signals matching optional regime filters (for 'System Proof' style)."""
    if df is None or df.empty:
        return {"n": 0}
    rows = []
    for _, r in df.iterrows():
        try:
            snap = json.loads(r.get("regime_snapshot") or "{}")
        except Exception:
            snap = {}
        rows.append(
            {
                "directional_win": r.get("directional_win"),
                "return_4w_pct": r.get("return_4w_pct"),
                "mae_pct": r.get("mae_pct"),
                "mfe_pct": r.get("mfe_pct"),
                "data_quality_overall": r.get("data_quality_overall"),
                "shock": str(snap.get("shock_type") or ""),
                "curve": str(snap.get("curve_signal") or ""),
                "shield_active": bool(snap.get("shield_active")),
                "dxy_trend": str(snap.get("dxy_trend") or "NEUTRAL").upper(),
                "scenario": str(snap.get("scenario") or ""),
            }
        )
    d = pd.DataFrame(rows)
    if shock_type:
        d = d[d["shock"] == str(shock_type)]
    if curve_signal:
        d = d[d["curve"] == str(curve_signal)]
    if shield_active is not None:
        d = d[d["shield_active"] == bool(shield_active)]
    if dxy_trend and str(dxy_trend).strip().upper() not in ("(ANY)", "ANY", ""):
        d = d[d["dxy_trend"] == str(dxy_trend).strip().upper()]
    if scenario_in:
        want = {str(x) for x in scenario_in if str(x).strip()}
        if want:
            d = d[d["scenario"].isin(want)]
    n = len(d)
    if n == 0:
        return {"n": 0}
    wr = float(d["directional_win"].mean() * 100.0) if d["directional_win"].notna().any() else None
    return {
        "n": n,
        "win_rate_pct": wr,
        "avg_mae_pct": float(d["mae_pct"].mean()) if d["mae_pct"].notna().any() else None,
        "avg_mfe_pct": float(d["mfe_pct"].mean()) if d["mfe_pct"].notna().any() else None,
        "avg_data_quality": float(d["data_quality_overall"].mean()) if d["data_quality_overall"].notna().any() else None,
    }


def build_regime_snapshot_for_ledger(
    *,
    shock_type: str,
    scenario: str,
    curve_signal: str,
    shield_data: Optional[dict],
    vix: float,
    rms_gap: Optional[float] = None,
) -> dict[str, Any]:
    """Compact JSON-safe snapshot at T0 for ledger / regime matching."""
    sh = shield_data or {}
    shield_active = bool(sh.get("VIX_Shock") or sh.get("OVX_Shock"))
    out: dict[str, Any] = {
        "shock_type": str(shock_type or "NONE"),
        "scenario": str(scenario or ""),
        "curve_signal": str(curve_signal or "N/A"),
        "vix": float(vix or 0.0),
        "shield_active": shield_active,
        "dxy_trend": str(sh.get("DXY_Trend", "NEUTRAL")),
    }
    if rms_gap is not None:
        out["rms_gap"] = float(rms_gap)
    return out

from typing import Any

import pandas as pd
import yfinance as yf
from datetime import timedelta, timezone

from .audit_metrics import ADJUSTED_WIN_MFE_THRESHOLD_PCT, bias_to_direction, compute_directional_excursions
from .config import DB_NAME, TICKER_MAP, DISPLAY_MAP  # DB_NAME: tests monkeypatch sqlite path
from .db_backend import get_connection, read_sql_pandas
from .retry_http import yfinance_download_retry

# Samo prave ISO-valute (3 znaka); sprečava npr. "SILVER" → SIL+VER → SILVER=X (nevažeći Yahoo simbol).
_FX_CCY_3 = frozenset(
    {
        "USD",
        "EUR",
        "GBP",
        "JPY",
        "CHF",
        "AUD",
        "NZD",
        "CAD",
        "CNY",
        "CNH",
        "HKD",
        "SGD",
        "MXN",
        "SEK",
        "NOK",
        "DKK",
        "PLN",
        "ZAR",
    }
)


def _resolve_ticker_for_audit(inst_value, lookup_map):
    inst = str(inst_value or "").strip().upper()
    if not inst:
        return None

    # 1) Direct lookup (full name / short code aliases from config).
    direct = lookup_map.get(inst_value) or lookup_map.get(inst)
    if direct:
        return direct

    # 2) Normalize separators/spaces and retry.
    compact = inst.replace(" ", "")
    compact = compact.replace("-", "")
    if compact in lookup_map:
        return lookup_map[compact]

    # 3) FX pair parsing (e.g. EUR/AUD, EURAUD, GBP/USD).
    pair = None
    if "/" in inst:
        parts = [p.strip() for p in inst.split("/") if p.strip()]
        if len(parts) == 2 and len(parts[0]) == 3 and len(parts[1]) == 3:
            pair = (parts[0], parts[1])
    elif len(compact) == 6 and compact.isalpha():
        b3, q3 = compact[:3], compact[3:]
        if b3 in _FX_CCY_3 and q3 in _FX_CCY_3:
            pair = (b3, q3)

    if not pair:
        return None

    base, quote = pair
    # Yahoo special format for USDXXX majors in this codebase.
    if base == "USD" and quote in {"JPY", "CAD", "CHF"}:
        return f"{quote}=X"
    return f"{base}{quote}=X"


def _ohlc_series_from_download(df_price, ticker):
    if df_price.empty:
        return None, None, None, None
    if isinstance(df_price.columns, pd.MultiIndex):
        hi = df_price["High"][ticker].dropna()
        lo = df_price["Low"][ticker].dropna()
        op = df_price["Open"][ticker].dropna()
        cl = df_price["Close"][ticker].dropna()
    else:
        hi = df_price["High"].dropna()
        lo = df_price["Low"].dropna()
        op = df_price["Open"].dropna()
        cl = df_price["Close"].dropna()
    return hi, lo, op, cl


def get_stats_v5():
    """
    Single success criterion: `adjusted_win` / `win_loss` both reflect **efficiency win**
    (return in bias direction > 0 OR MFE > ADJUSTED_WIN_MFE_THRESHOLD_PCT). NEUTRAL → no win flag.
    """
    try:
        stats_q = """
                SELECT 
                    COUNT(*) as total, 
                    COALESCE(SUM(CASE WHEN win_loss='WIN' THEN 1 ELSE 0 END), 0) as wins, 
                    COALESCE(SUM(CASE WHEN win_loss='LOSS' THEN 1 ELSE 0 END), 0) as losses, 
                    COALESCE(SUM(CASE WHEN win_loss='NEUTRAL' THEN 1 ELSE 0 END), 0) as neutral_count,
                    COALESCE(AVG(master_score), 0) as avg_score 
                FROM signal_tracker 
                WHERE status='CLOSED'
            """
        df_s = read_sql_pandas(stats_q)
        df_all = read_sql_pandas("SELECT * FROM signal_tracker ORDER BY timestamp DESC")
        if not df_s.empty:
            df_s = df_s.copy()
            if int(df_s["total"].iloc[0]) > 0:
                w = int(df_s["wins"].iloc[0])
                l = int(df_s["losses"].iloc[0])
                n_dir = w + l
                df_s["quant_efficiency_win_rate_pct"] = (
                    float(w) / float(n_dir) * 100.0 if n_dir > 0 else float("nan")
                )
                df_s["adjusted_win_rate_pct"] = df_s["quant_efficiency_win_rate_pct"]
                df_s["adjusted_wins"] = w
                df_s["adjusted_evaluated_n"] = n_dir
                df_s["losses_evaluated"] = l
            else:
                df_s["quant_efficiency_win_rate_pct"] = float("nan")
                df_s["adjusted_win_rate_pct"] = float("nan")
                df_s["adjusted_wins"] = 0
                df_s["adjusted_evaluated_n"] = 0
                df_s["losses_evaluated"] = 0
        return df_s, df_all
    except Exception as e:
        print(f"Baza greška u get_stats_v5: {e}")
        return pd.DataFrame(), pd.DataFrame()


def run_auto_audit():
    """
    Weekly (1W) close: OHLC Mon–Fri → `compute_directional_excursions` (same formula as 4W ledger).
    Populates mfe_pct, mae_pct, efficiency_ratio, adjusted_win; win_loss mirrors efficiency (WIN/LOSS/NEUTRAL).
    """
    try:
        open_signals = read_sql_pandas("SELECT * FROM signal_tracker WHERE status='OPEN'")
        if open_signals.empty:
            return 0, "Nema otvorenih signala za reviziju."

        with get_connection() as conn:

            closed_count = 0
            waiting_for_close_count = 0
            missing_data_count = 0
            unresolved_ticker_count = 0
            now = pd.Timestamp.now(tz="UTC")

            lookup_map = {}
            for full_name, ticker in TICKER_MAP.items():
                short_code = DISPLAY_MAP.get(full_name, "")
                lookup_map[full_name] = ticker
                lookup_map[str(full_name).upper()] = ticker
                if short_code:
                    lookup_map[short_code] = ticker
                    lookup_map[str(short_code).upper()] = ticker

            for _, row in open_signals.iterrows():
                sig_id = row["id"]
                inst = row["instrument"]
                bias = str(row["predicted_bias"]).upper()
                sig_time = pd.to_datetime(row["timestamp"], errors="coerce", utc=True)
                if pd.isna(sig_time):
                    continue

                if sig_time.weekday() >= 5:
                    target_monday = sig_time + timedelta(days=(7 - sig_time.weekday()))
                else:
                    target_monday = sig_time - timedelta(days=sig_time.weekday())

                target_monday = target_monday.tz_convert("UTC").replace(hour=0, minute=0, second=0, microsecond=0)
                target_friday = target_monday + timedelta(days=4)
                # Trading week ends at Friday market close (approx 21:00 UTC for US session).
                target_friday_close = target_friday.replace(hour=21, minute=0, second=0, microsecond=0)

                if now < target_friday_close:
                    waiting_for_close_count += 1
                    continue

                ticker = _resolve_ticker_for_audit(inst, lookup_map)
                if not ticker:
                    unresolved_ticker_count += 1
                    continue

                start_date_str = target_monday.strftime("%Y-%m-%d")
                end_date_str = (target_friday + timedelta(days=3)).strftime("%Y-%m-%d")

                try:
                    df_price = yfinance_download_retry(
                        lambda: yf.download(
                            ticker,
                            start=start_date_str,
                            end=end_date_str,
                            interval="1d",
                            progress=False,
                        ),
                        attempts=4,
                        base_seconds=2.0,
                        default=pd.DataFrame(),
                    )
                    hi, lo, opens, prices = _ohlc_series_from_download(df_price, ticker)
                    if hi is None or hi.empty or lo.empty or prices.empty or opens.empty:
                        missing_data_count += 1
                        continue

                    t0 = target_monday.tz_localize(None)
                    t1 = target_friday.tz_localize(None)
                    mask = (hi.index >= t0) & (hi.index <= t1)
                    hi_w = hi.loc[mask]
                    lo_w = lo.loc[mask]
                    cl_w = prices.loc[mask]
                    if hi_w.empty or lo_w.empty or cl_w.empty:
                        missing_data_count += 1
                        continue

                    p_max = float(hi_w.max())
                    p_min = float(lo_w.min())
                    close_price = float(cl_w.iloc[-1])

                    raw_entry = row.get("entry_price", None)
                    entry_price = (
                        float(raw_entry)
                        if raw_entry is not None and not pd.isna(raw_entry) and float(raw_entry) > 0
                        else float(opens.loc[opens.index >= t0].iloc[0])
                        if not opens.loc[opens.index >= t0].empty
                        else float(opens.iloc[0])
                    )

                    direction = bias_to_direction(bias)
                    ret_pct, mfe_pct, mae_pct, er, eff_w = compute_directional_excursions(
                        entry_price, p_min, p_max, close_price, direction
                    )

                    neutral_row = direction == "NEUTRAL" or "NEUTRAL" in bias

                    if neutral_row:
                        outcome = "NEUTRAL"
                    elif eff_w == 1:
                        outcome = "WIN"
                    else:
                        outcome = "LOSS"

                    if neutral_row:
                        mfe_sql = None
                        mae_sql = None
                        er_sql = None
                        adj_sql = None
                    else:
                        mfe_sql = float(mfe_pct)
                        mae_sql = float(mae_pct)
                        er_sql = float(er)
                        adj_sql = int(eff_w) if eff_w is not None else None

                    conn.execute(
                        """
                        UPDATE signal_tracker
                        SET entry_price=?, result_price=?, win_loss=?, status='CLOSED',
                            mfe_pct=?, mae_pct=?, efficiency_ratio=?, adjusted_win=?
                        WHERE id=?
                        """,
                        (
                            entry_price,
                            close_price,
                            outcome,
                            mfe_sql,
                            mae_sql,
                            er_sql,
                            adj_sql,
                            sig_id,
                        ),
                    )
                    conn.commit()
                    closed_count += 1

                except Exception as e:
                    print(f"Audit Error [{inst}]: {e}")
                    continue

        if closed_count > 0:
            return closed_count, f"Revizija završena. Zatvoreno: {closed_count}."
        if waiting_for_close_count > 0:
            return 0, "Nema signala spremnih za zatvaranje (čekamo Friday market close)."
        if missing_data_count > 0:
            return 0, "Nema signala spremnih za zatvaranje (tržišni podaci još nisu kompletni)."
        if unresolved_ticker_count > 0:
            return 0, "Nema signala spremnih za zatvaranje (instrument ticker mapiranje nedostaje)."
        return 0, "Nema signala spremnih za zatvaranje."

    except Exception as e:
        return 0, f"Kritična greška u Audit Engine: {e}"


def backfill_closed_signal_tracker_metrics(*, max_rows: int = 500) -> tuple[int, str]:
    """
    Recompute MFE/MAE/ER/adjusted_win for CLOSED rows missing efficiency_ratio (or forced refresh).
    Uses same calendar week window as run_auto_audit.
    """
    lookup_map = {}
    for full_name, ticker in TICKER_MAP.items():
        short_code = DISPLAY_MAP.get(full_name, "")
        lookup_map[full_name] = ticker
        lookup_map[str(full_name).upper()] = ticker
        if short_code:
            lookup_map[short_code] = ticker
            lookup_map[str(short_code).upper()] = ticker

    updated = 0
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, instrument, predicted_bias, entry_price, result_price, timestamp, win_loss
                FROM signal_tracker
                WHERE status='CLOSED'
                  AND (efficiency_ratio IS NULL OR mfe_pct IS NULL)
                ORDER BY id ASC
                LIMIT ?
                """,
                (int(max_rows),),
            ).fetchall()
            for sig_id, inst, bias, entry_p, result_p, ts, _wl in rows:
                bias_s = str(bias or "").upper()
                sig_time = pd.to_datetime(ts, errors="coerce", utc=True)
                if pd.isna(sig_time):
                    continue
                if sig_time.weekday() >= 5:
                    target_monday = sig_time + timedelta(days=(7 - sig_time.weekday()))
                else:
                    target_monday = sig_time - timedelta(days=sig_time.weekday())
                target_monday = target_monday.tz_convert("UTC").replace(hour=0, minute=0, second=0, microsecond=0)
                target_friday = target_monday + timedelta(days=4)
                ticker = _resolve_ticker_for_audit(inst, lookup_map)
                if not ticker:
                    continue
                start_date_str = target_monday.strftime("%Y-%m-%d")
                end_date_str = (target_friday + timedelta(days=3)).strftime("%Y-%m-%d")
                try:
                    df_price = yfinance_download_retry(
                        lambda: yf.download(
                            ticker,
                            start=start_date_str,
                            end=end_date_str,
                            interval="1d",
                            progress=False,
                        ),
                        attempts=4,
                        base_seconds=2.0,
                        default=pd.DataFrame(),
                    )
                    hi, lo, opens, prices = _ohlc_series_from_download(df_price, ticker)
                    if hi is None or hi.empty:
                        continue
                    t0 = target_monday.tz_localize(None)
                    t1 = target_friday.tz_localize(None)
                    mask = (hi.index >= t0) & (hi.index <= t1)
                    hi_w = hi.loc[mask]
                    lo_w = lo.loc[mask]
                    cl_w = prices.loc[mask]
                    if hi_w.empty:
                        continue
                    p_max = float(hi_w.max())
                    p_min = float(lo_w.min())
                    close_price = float(result_p) if result_p is not None and float(result_p) > 0 else float(cl_w.iloc[-1])
                    try:
                        entry_price = (
                            float(entry_p)
                            if entry_p is not None and not pd.isna(entry_p) and float(entry_p) > 0
                            else float(opens.loc[opens.index >= t0].iloc[0])
                        )
                    except Exception:
                        entry_price = float(opens.iloc[0])
                    direction = bias_to_direction(bias_s)
                    if direction == "NEUTRAL" or "NEUTRAL" in bias_s:
                        conn.execute(
                            """
                            UPDATE signal_tracker SET mfe_pct=NULL, mae_pct=NULL, efficiency_ratio=NULL, adjusted_win=NULL,
                              win_loss='NEUTRAL'
                            WHERE id=?
                            """,
                            (int(sig_id),),
                        )
                    else:
                        ret_pct, mfe_pct, mae_pct, er, eff_w = compute_directional_excursions(
                            entry_price, p_min, p_max, close_price, direction
                        )
                        wl = "WIN" if eff_w == 1 else "LOSS"
                        conn.execute(
                            """
                            UPDATE signal_tracker SET mfe_pct=?, mae_pct=?, efficiency_ratio=?, adjusted_win=?,
                              win_loss=?
                            WHERE id=?
                            """,
                            (
                                mfe_pct,
                                mae_pct,
                                er,
                                int(eff_w) if eff_w is not None else None,
                                wl,
                                int(sig_id),
                            ),
                        )
                    updated += 1
                except Exception:
                    continue
            conn.commit()
        return updated, f"Backfilled {updated} closed signal_tracker row(s)."
    except Exception as e:
        return 0, str(e)


def sync_win_loss_from_adjusted_signal_tracker(*, max_rows: Any = None) -> tuple[int, str]:
    """
    Retroactively set win_loss from adjusted_win for CLOSED rows (efficiency = primary).
    NEUTRAL bias rows: win_loss='NEUTRAL', adjusted_win NULL.
    """
    try:
        with get_connection() as conn:
            if max_rows is not None:
                rows = conn.execute(
                    """
                    SELECT id, predicted_bias, adjusted_win
                    FROM signal_tracker
                    WHERE status='CLOSED'
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (int(max_rows),),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, predicted_bias, adjusted_win
                    FROM signal_tracker
                    WHERE status='CLOSED'
                    ORDER BY id ASC
                    """
                ).fetchall()
            n = 0
            for rid, bias, adj in rows:
                bs = str(bias or "").upper()
                if "NEUTRAL" in bs or not any(
                    x in bs for x in ("LONG", "BUY", "BULL", "SHORT", "SELL", "BEAR")
                ):
                    conn.execute(
                        "UPDATE signal_tracker SET win_loss='NEUTRAL' WHERE id=?",
                        (int(rid),),
                    )
                    n += 1
                    continue
                if adj is None:
                    continue
                wl = "WIN" if int(adj) == 1 else "LOSS"
                conn.execute(
                    "UPDATE signal_tracker SET win_loss=? WHERE id=?",
                    (wl, int(rid)),
                )
                n += 1
            conn.commit()
        return n, f"sync_win_loss_from_adjusted: updated {n} row(s)."
    except Exception as e:
        return 0, str(e)


def verify_signal_tracker_win_alignment() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Rows where directional CLOSED win_loss disagrees with adjusted_win (should be empty after sync).
    """
    import pandas as pd

    try:
        df = read_sql_pandas(
            """
                SELECT id, instrument, predicted_bias, win_loss, adjusted_win
                FROM signal_tracker
                WHERE status='CLOSED'
                """
        )
    except Exception:
        return [], {"error": "read failed"}

    if df.empty:
        return [], {"total_closed": 0}

    mism = []
    neutral_bad = []
    for _, r in df.iterrows():
        bias = str(r.get("predicted_bias") or "").upper()
        wl = str(r.get("win_loss") or "")
        adj = r.get("adjusted_win")
        is_neutral_bias = "NEUTRAL" in bias or not any(
            x in bias for x in ("LONG", "BUY", "BULL", "SHORT", "SELL", "BEAR")
        )
        if is_neutral_bias:
            if wl not in ("NEUTRAL", ""):
                neutral_bad.append(dict(r))
            continue
        if adj is None or pd.isna(adj):
            continue
        want = "WIN" if int(adj) == 1 else "LOSS"
        if wl != want:
            mism.append(dict(r))
    return mism + neutral_bad, {
        "total_closed": len(df),
        "mismatch_count": len(mism) + len(neutral_bad),
    }


def print_audit_efficiency_comparison_table() -> None:
    """Terminal: Asset | Quant Efficiency WR% | win_loss vs adjusted_win mismatches | Avg ER."""
    _, df = get_stats_v5()
    if df is None or df.empty:
        print("No signal_tracker data.")
        return
    sub = df[df["status"] == "CLOSED"].copy()
    if sub.empty:
        print("No CLOSED rows.")
        return
    print(f"MFE threshold (efficiency win): {ADJUSTED_WIN_MFE_THRESHOLD_PCT}%")
    print(f"{'Asset':<28} | {'N_dir':>5} | {'Eff WR%':>9} | {'mis WL≠adj':>10} | {'Avg ER':>10}")
    print("-" * 78)
    for inst, g in sub.groupby(sub["instrument"].astype(str), dropna=False):
        bull = g["predicted_bias"].astype(str).str.upper()
        is_dir = bull.str.contains("LONG|BUY|BULL|SHORT|SELL|BEAR", regex=True)
        g2 = g[is_dir]
        if g2.empty:
            continue
        n2 = len(g2)
        w = (g2["win_loss"] == "WIN").sum()
        wr = float(w) / float(n2) * 100.0 if n2 else 0.0
        mis = 0
        for _, r in g2.iterrows():
            adj = r.get("adjusted_win")
            if adj is None or pd.isna(adj):
                continue
            want = "WIN" if int(adj) == 1 else "LOSS"
            if str(r.get("win_loss") or "") != want:
                mis += 1
        er_col = g2["efficiency_ratio"].dropna() if "efficiency_ratio" in g2.columns else pd.Series(dtype=float)
        avg_er = float(er_col.mean()) if not er_col.empty else float("nan")
        print(f"{str(inst)[:28]:<28} | {n2:5d} | {wr:9.2f} | {mis:10d} | {avg_er:10.4f}")

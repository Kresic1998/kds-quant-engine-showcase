import json
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from . import config as _cfg
from .db_backend import get_connection, read_sql_pandas, use_postgresql
from .pg_schema import ensure_postgres_schema, postgres_add_missing_columns


def migrate_audit_tables(conn: Optional[sqlite3.Connection] = None) -> None:
    """
    Add MFE/MAE/Efficiency/adjusted win columns to signal_tracker and system_performance_ledger.
    Safe to call repeatedly (IF NOT EXISTS via try/except).
    """
    if use_postgresql():
        postgres_add_missing_columns()
        return

    close_conn = False
    if conn is None:
        conn = sqlite3.connect(_cfg.get_sqlite_db_path())
        close_conn = True
    try:
        for table in ("signal_tracker", "system_performance_ledger"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            alters = [
                ("mfe_pct", "REAL"),
                ("mae_pct", "REAL"),
                ("efficiency_ratio", "REAL"),
                ("adjusted_win", "INTEGER"),
            ]
            for col, typ in alters:
                if col not in cols:
                    try:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
                    except sqlite3.OperationalError:
                        pass
        conn.commit()
    finally:
        if close_conn:
            conn.close()


def sql_sync_signal_tracker_win_loss_from_adjusted(conn: Optional[sqlite3.Connection] = None) -> int:
    """
    Bulk SQL: set `win_loss` from `adjusted_win` for CLOSED rows (1W = 4W efficiency standard).
    NEUTRAL / non-directional bias → 'NEUTRAL'. Directional: WIN/LOSS from adjusted_win 1/0.
    Rows with NULL adjusted_win are left unchanged.
    """
    sql_sqlite = """
            UPDATE signal_tracker
            SET win_loss = CASE
                WHEN instr(upper(coalesce(predicted_bias, '')), 'NEUTRAL') > 0 THEN 'NEUTRAL'
                WHEN NOT (
                    instr(upper(coalesce(predicted_bias, '')), 'LONG') > 0 OR
                    instr(upper(coalesce(predicted_bias, '')), 'BUY') > 0 OR
                    instr(upper(coalesce(predicted_bias, '')), 'BULL') > 0 OR
                    instr(upper(coalesce(predicted_bias, '')), 'SHORT') > 0 OR
                    instr(upper(coalesce(predicted_bias, '')), 'SELL') > 0 OR
                    instr(upper(coalesce(predicted_bias, '')), 'BEAR') > 0
                ) THEN 'NEUTRAL'
                WHEN adjusted_win = 1 THEN 'WIN'
                WHEN adjusted_win = 0 THEN 'LOSS'
                ELSE win_loss
            END
            WHERE status = 'CLOSED'
            """
    sql_pg = """
            UPDATE signal_tracker
            SET win_loss = CASE
                WHEN strpos(upper(coalesce(predicted_bias, '')), 'NEUTRAL') > 0 THEN 'NEUTRAL'
                WHEN NOT (
                    strpos(upper(coalesce(predicted_bias, '')), 'LONG') > 0 OR
                    strpos(upper(coalesce(predicted_bias, '')), 'BUY') > 0 OR
                    strpos(upper(coalesce(predicted_bias, '')), 'BULL') > 0 OR
                    strpos(upper(coalesce(predicted_bias, '')), 'SHORT') > 0 OR
                    strpos(upper(coalesce(predicted_bias, '')), 'SELL') > 0 OR
                    strpos(upper(coalesce(predicted_bias, '')), 'BEAR') > 0
                ) THEN 'NEUTRAL'
                WHEN adjusted_win = 1 THEN 'WIN'
                WHEN adjusted_win = 0 THEN 'LOSS'
                ELSE win_loss
            END
            WHERE status = 'CLOSED'
            """

    if use_postgresql():
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_pg.strip())
            n = cur.rowcount
            conn.commit()
            return int(n)

    close_conn = False
    if conn is None:
        conn = sqlite3.connect(_cfg.get_sqlite_db_path())
        close_conn = True
    try:
        cur = conn.execute(sql_sqlite.strip())
        n = cur.rowcount if cur.rowcount is not None else -1
        conn.commit()
        return int(n)
    finally:
        if close_conn:
            conn.close()


def _ensure_daily_snapshots_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_at TEXT NOT NULL,
            instrument_kind TEXT NOT NULL,
            instrument_code TEXT NOT NULL,
            master_score REAL,
            verdict_or_signal TEXT,
            spec_z REAL,
            z_spread REAL,
            scenario TEXT,
            shock_type TEXT,
            lookback INTEGER,
            panel_json TEXT,
            score_24h_reference REAL,
            score_decomposition_json TEXT,
            UNIQUE(snapshot_at, instrument_kind, instrument_code)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_snapshots_at ON daily_snapshots(snapshot_at)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_daily_snapshots_code ON daily_snapshots(instrument_code, snapshot_at)"
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(daily_snapshots)").fetchall()}
    if "score_24h_reference" not in cols:
        try:
            conn.execute("ALTER TABLE daily_snapshots ADD COLUMN score_24h_reference REAL")
        except sqlite3.OperationalError:
            pass
    if "score_decomposition_json" not in cols:
        try:
            conn.execute("ALTER TABLE daily_snapshots ADD COLUMN score_decomposition_json TEXT")
        except sqlite3.OperationalError:
            pass


def _migrate_perf_ledger_nullable_signal(conn: sqlite3.Connection) -> None:
    """
    Older DBs had NOT NULL signal_tracker_id + no backfill_source.
    Rebuild table once to allow historical ledger rows without signal_tracker.
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='system_performance_ledger'"
    ).fetchone()
    if not row or not row[0]:
        return
    ddl = row[0]
    if "backfill_source" in ddl and "signal_tracker_id INTEGER NOT NULL" not in ddl:
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS system_performance_ledger__new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_tracker_id INTEGER,
            created_at DATETIME,
            instrument TEXT,
            signal_type TEXT,
            master_score REAL,
            predicted_bias TEXT,
            p_entry REAL,
            regime_snapshot TEXT,
            data_quality_overall REAL,
            horizon_end_4w TEXT,
            p_max_4w REAL,
            p_min_4w REAL,
            p_close_4w REAL,
            return_4w_pct REAL,
            mfe_pct REAL,
            mae_pct REAL,
            directional_win INTEGER,
            status TEXT DEFAULT 'PENDING',
            backfill_source TEXT,
            FOREIGN KEY(signal_tracker_id) REFERENCES signal_tracker(id)
        )
        """
    )
    cols_old = [r[1] for r in conn.execute("PRAGMA table_info(system_performance_ledger)").fetchall()]
    if cols_old:
        common = [
            c
            for c in (
                "id",
                "signal_tracker_id",
                "created_at",
                "instrument",
                "signal_type",
                "master_score",
                "predicted_bias",
                "p_entry",
                "regime_snapshot",
                "data_quality_overall",
                "horizon_end_4w",
                "p_max_4w",
                "p_min_4w",
                "p_close_4w",
                "return_4w_pct",
                "mfe_pct",
                "mae_pct",
                "directional_win",
                "status",
            )
            if c in cols_old
        ]
        if common:
            sel = ", ".join(common)
            conn.execute(
                f"INSERT INTO system_performance_ledger__new ({sel}) SELECT {sel} FROM system_performance_ledger"
            )
    conn.execute("DROP TABLE system_performance_ledger")
    conn.execute("ALTER TABLE system_performance_ledger__new RENAME TO system_performance_ledger")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_perf_ledger_status_horizon ON system_performance_ledger(status, horizon_end_4w)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_perf_ledger_signal_id ON system_performance_ledger(signal_tracker_id)"
    )
    conn.execute("PRAGMA foreign_keys=ON")


def ensure_sqlite_database_file_ready() -> None:
    """
    If the resolved SQLite path has no file yet, copy bundled `cot_quant_master.db` from the
    repo root (GitHub seed) so Cloud users start with COT + ledger without re-downloading everything.

    Ongoing CFTC/Yahoo updates always write to `get_sqlite_db_path()` — use Secrets to point that
    path at persistent storage so redeploys do not wipe client data.
    """
    db_path_str = str(_cfg.get_sqlite_db_path())
    target = Path(db_path_str).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and target.stat().st_size > 0:
        return
    if target.exists():
        try:
            target.unlink()
        except OSError:
            pass
    repo_root = Path(__file__).resolve().parent.parent
    seed = repo_root / "cot_quant_master.db"
    if seed.is_file() and seed.stat().st_size > 0:
        try:
            shutil.copy2(seed, target)
        except OSError:
            pass
    # Repo seed is often gitignored (*.db); CI has no file to copy — still need a real DB path.
    if not target.exists() or target.stat().st_size == 0:
        if target.exists():
            try:
                target.unlink()
            except OSError:
                pass
        try:
            with sqlite3.connect(str(target)) as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS _bootstrap (id INTEGER PRIMARY KEY)")
                conn.execute("DROP TABLE _bootstrap")
        except OSError:
            pass


def init_signal_db():
    if use_postgresql():
        ensure_postgres_schema()
        postgres_add_missing_columns()
        migrate_audit_tables()
        return

    ensure_sqlite_database_file_ready()
    with sqlite3.connect(_cfg.get_sqlite_db_path()) as conn:
        query = """
        CREATE TABLE IF NOT EXISTS signal_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            instrument TEXT,
            entry_price REAL,
            predicted_bias TEXT,
            master_score REAL,
            status TEXT DEFAULT 'OPEN',
            result_price REAL,
            win_loss TEXT,
            ai_note TEXT,
            is_verified INTEGER DEFAULT 0
        )
        """
        conn.execute(query)
        
        try:
            conn.execute("ALTER TABLE signal_tracker ADD COLUMN ai_note TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE signal_tracker ADD COLUMN is_verified INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
            
        conn.commit()

        # Persistencija AI sweep rezultata (swing modul)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS swing_sweep_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME,
            run_key TEXT,
            scenario TEXT,
            app_bias TEXT,
            sweep_limit INTEGER,
            use_prefilter INTEGER,
            notes TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS swing_sweep_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            asset TEXT,
            bias TEXT,
            confidence REAL,
            clarity_score REAL,
            prefilter_score REAL,
            stop_zone TEXT,
            target_zone TEXT,
            plan_note TEXT,
            FOREIGN KEY(run_id) REFERENCES swing_sweep_runs(id)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS weekend_trade_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME,
            week_start DATE,
            instrument TEXT,
            bias TEXT,
            setup_type TEXT,
            tv_chart_url TEXT,
            key_levels TEXT,
            invalidation TEXT,
            entry_idea TEXT,
            risk_plan TEXT,
            execution_notes TEXT,
            status TEXT DEFAULT 'ACTIVE'
        )
        """)
        try:
            conn.execute("ALTER TABLE weekend_trade_plans ADD COLUMN tv_chart_url TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE weekend_trade_plans ADD COLUMN user_note TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE weekend_trade_plans ADD COLUMN last_update_kind TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE weekend_trade_plans ADD COLUMN last_update_note TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE weekend_trade_plans ADD COLUMN last_update_tv_url TEXT")
        except sqlite3.OperationalError:
            pass
        # DB optimizations / integrity constraints.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracker_instrument_ts ON signal_tracker(instrument, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_tracker_status_ts ON signal_tracker(status, timestamp)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_weekend_plan_week_instrument ON weekend_trade_plans(week_start, instrument)")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS system_performance_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_tracker_id INTEGER,
            created_at DATETIME,
            instrument TEXT,
            signal_type TEXT,
            master_score REAL,
            predicted_bias TEXT,
            p_entry REAL,
            regime_snapshot TEXT,
            data_quality_overall REAL,
            horizon_end_4w TEXT,
            p_max_4w REAL,
            p_min_4w REAL,
            p_close_4w REAL,
            return_4w_pct REAL,
            mfe_pct REAL,
            mae_pct REAL,
            directional_win INTEGER,
            status TEXT DEFAULT 'PENDING',
            backfill_source TEXT,
            FOREIGN KEY(signal_tracker_id) REFERENCES signal_tracker(id)
        )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_perf_ledger_status_horizon ON system_performance_ledger(status, horizon_end_4w)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_perf_ledger_signal_id ON system_performance_ledger(signal_tracker_id)"
        )
        _migrate_perf_ledger_nullable_signal(conn)
        _ensure_daily_snapshots_table(conn)
        migrate_audit_tables(conn)
        # Bolje paralelno čitanje tokom upisa (Streamlit + testovi / lokalni alati).
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        conn.commit()


def log_ai_signal(instrument, price, bias, score, ai_note="", ledger_context=None):
    """
    ledger_context: optional dict with keys:
      signal_type ('single'|'rv'), regime_snapshot (dict), data_quality_overall (float)
    Inserts system_performance_ledger row linked to signal_tracker for calibration / MAE-MFE.
    """
    try:
        now = datetime.now()
        # Jedan zapis po instrumentu po kalendarskoj nedelji (pon–ned) — sprečava duplikate pri ponovnom pokretanju skripte.
        week_start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        week_end = week_start + timedelta(days=7)
        with get_connection() as conn:
            if not use_postgresql():
                try:
                    conn.execute("PRAGMA foreign_keys=ON")
                except Exception:
                    pass
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT 1 FROM signal_tracker
                WHERE instrument = ? AND timestamp >= ? AND timestamp < ?
                LIMIT 1
                """,
                (instrument, week_start, week_end),
            )
            if cursor.fetchone():
                return True
            cursor.execute("""
                INSERT INTO signal_tracker (timestamp, instrument, entry_price, predicted_bias, master_score, ai_note)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (now, instrument, price, bias, score, ai_note))
            sig_id = int(cursor.lastrowid)
            ctx = ledger_context if isinstance(ledger_context, dict) else None
            if ctx:
                snap = ctx.get("regime_snapshot")
                if isinstance(snap, dict):
                    snap_s = json.dumps(snap, ensure_ascii=False)
                else:
                    snap_s = str(snap or "{}")
                horizon_end = (now + timedelta(days=28)).date().isoformat()
                dq = ctx.get("data_quality_overall")
                dq_f = float(dq) if dq is not None and dq == dq else None
                cursor.execute(
                    """
                    INSERT INTO system_performance_ledger (
                        signal_tracker_id, created_at, instrument, signal_type, master_score,
                        predicted_bias, p_entry, regime_snapshot, data_quality_overall,
                        horizon_end_4w, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')
                    """,
                    (
                        sig_id,
                        now,
                        str(instrument),
                        str(ctx.get("signal_type") or "single"),
                        float(score),
                        str(bias),
                        float(price) if price is not None and price == price and float(price) > 0 else None,
                        snap_s,
                        dq_f,
                        horizon_end,
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"Greška pri logovanju signala u bazu: {e}")
        return False


def save_swing_sweep_results(results_df, scenario, app_bias, sweep_limit, use_prefilter, notes=""):
    """Snima jedan sweep run + njegove rezultate."""
    try:
        if results_df is None or results_df.empty:
            return False
        run_key = datetime.now().strftime("%Y-%m-%d")
        created_at = datetime.now()
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO swing_sweep_runs (created_at, run_key, scenario, app_bias, sweep_limit, use_prefilter, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (created_at, run_key, scenario, app_bias, int(sweep_limit), int(bool(use_prefilter)), notes),
            )
            run_id = cur.lastrowid

            rows = []
            for _, r in results_df.iterrows():
                rows.append(
                    (
                        run_id,
                        str(r.get("Asset", "")),
                        str(r.get("Bias", "WAIT")),
                        float(r.get("Confidence", 0.0)),
                        float(r.get("ClarityScore", 0.0)),
                        float(r.get("PrefilterScore", 0.0)),
                        str(r.get("StopZone", "")),
                        str(r.get("TargetZone", "")),
                        str(r.get("PlanNapomena", "")),
                    )
                )
            cur.executemany(
                """
                INSERT INTO swing_sweep_results
                (run_id, asset, bias, confidence, clarity_score, prefilter_score, stop_zone, target_zone, plan_note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        return True
    except Exception as e:
        print(f"Greška pri čuvanju swing sweep rezultata: {e}")
        return False


def get_latest_swing_sweep(limit_rows=10):
    """Vraća poslednji run metapodatke + top redove."""
    try:
        with get_connection() as conn:
            run = conn.execute(
                """
                SELECT id, created_at, scenario, app_bias, sweep_limit, use_prefilter, notes
                FROM swing_sweep_runs
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if not run:
                return None, None
            run_id = run[0]
            rows = conn.execute(
                """
                SELECT asset as Asset, bias as Bias, confidence as Confidence, clarity_score as ClarityScore,
                       prefilter_score as PrefilterScore, stop_zone as StopZone, target_zone as TargetZone,
                       plan_note as PlanNapomena
                FROM swing_sweep_results
                WHERE run_id = ?
                ORDER BY clarity_score DESC, confidence DESC
                LIMIT ?
                """,
                (run_id, int(limit_rows)),
            ).fetchall()
        run_meta = {
            "id": run[0],
            "created_at": run[1],
            "scenario": run[2],
            "app_bias": run[3],
            "sweep_limit": run[4],
            "use_prefilter": bool(run[5]),
            "notes": run[6] or "",
        }
        import pandas as pd
        df = pd.DataFrame(rows, columns=["Asset", "Bias", "Confidence", "ClarityScore", "PrefilterScore", "StopZone", "TargetZone", "PlanNapomena"])
        return run_meta, df
    except Exception as e:
        print(f"Greška pri čitanju poslednjeg sweep-a: {e}")
        return None, None


def save_weekend_trade_plan(
    week_start,
    instrument,
    bias,
    setup_type,
    tv_chart_url,
    key_levels,
    invalidation,
    entry_idea,
    risk_plan,
    execution_notes,
    status="ACTIVE",
    user_note="",
    last_update_kind=None,
    last_update_note=None,
    last_update_tv_url=None,
):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            existing = cur.execute(
                """
                SELECT id
                FROM weekend_trade_plans
                WHERE week_start = ? AND instrument = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(week_start), str(instrument)),
            ).fetchone()

            if existing:
                old_status_row = cur.execute(
                    "SELECT status FROM weekend_trade_plans WHERE id = ?",
                    (int(existing[0]),),
                ).fetchone()
                old_st = str(old_status_row[0]).upper() if old_status_row and old_status_row[0] else "ACTIVE"
                if old_st in ("WIN", "LOSE", "BE"):
                    print("weekend_trade_plans: odbijen UPDATE — plan je već zatvoren.")
                    return False
                luk_raw = str(last_update_kind).strip().upper() if last_update_kind is not None else ""
                if luk_raw == "CLOSE_TRADE":
                    st_new = str(status).upper().strip()
                    if st_new in ("WIN", "LOSE", "BE"):
                        final_status = st_new
                    else:
                        final_status = old_st if old_st in ("WIN", "LOSE", "BE") else "ACTIVE"
                elif old_st in ("WIN", "LOSE", "BE"):
                    final_status = old_st
                else:
                    final_status = str(status or "ACTIVE").upper().strip() or "ACTIVE"
                status = final_status
                if last_update_kind is not None:
                    _luk = str(last_update_kind).strip() or None
                    _lun = str(last_update_note or "").strip() or None
                    _lutv = str(last_update_tv_url or "").strip() or None
                else:
                    old_meta = cur.execute(
                        "SELECT last_update_kind, last_update_note, last_update_tv_url FROM weekend_trade_plans WHERE id = ?",
                        (int(existing[0]),),
                    ).fetchone()
                    _luk = old_meta[0] if old_meta else None
                    _lun = old_meta[1] if old_meta and old_meta[1] else None
                    _lutv = old_meta[2] if old_meta and len(old_meta) > 2 and old_meta[2] else None
                cur.execute(
                    """
                    UPDATE weekend_trade_plans
                    SET created_at = ?, bias = ?, setup_type = ?, tv_chart_url = ?,
                        key_levels = ?, invalidation = ?, entry_idea = ?, risk_plan = ?,
                        execution_notes = ?, status = ?, user_note = ?,
                        last_update_kind = ?, last_update_note = ?, last_update_tv_url = ?
                    WHERE id = ?
                    """,
                    (
                        datetime.now(),
                        str(bias),
                        str(setup_type),
                        str(tv_chart_url),
                        str(key_levels),
                        str(invalidation),
                        str(entry_idea),
                        str(risk_plan),
                        str(execution_notes),
                        str(status),
                        str(user_note or ""),
                        _luk or None,
                        _lun or None,
                        _lutv,
                        int(existing[0]),
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO weekend_trade_plans
                    (created_at, week_start, instrument, bias, setup_type, tv_chart_url, key_levels, invalidation, entry_idea, risk_plan, execution_notes, status, user_note, last_update_kind, last_update_note, last_update_tv_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        datetime.now(),
                        str(week_start),
                        str(instrument),
                        str(bias),
                        str(setup_type),
                        str(tv_chart_url),
                        str(key_levels),
                        str(invalidation),
                        str(entry_idea),
                        str(risk_plan),
                        str(execution_notes),
                        str(status),
                        str(user_note or ""),
                        None,
                        None,
                        None,
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"Greška pri čuvanju weekend plana: {e}")
        return False


def get_weekend_trade_plans(limit_rows=60):
    try:
        with get_connection() as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, week_start, instrument, bias, setup_type, key_levels,
                       tv_chart_url, invalidation, entry_idea, risk_plan, execution_notes, status, user_note,
                       last_update_kind, last_update_note, last_update_tv_url
                FROM weekend_trade_plans
                ORDER BY week_start DESC, id DESC
                LIMIT ?
                """,
                (int(limit_rows),),
            ).fetchall()
        import pandas as pd
        return pd.DataFrame(
            rows,
            columns=[
                "id", "created_at", "week_start", "instrument", "bias", "setup_type", "key_levels",
                "tv_chart_url", "invalidation", "entry_idea", "risk_plan", "execution_notes", "status", "user_note",
                "last_update_kind", "last_update_note", "last_update_tv_url",
            ],
        )
    except Exception as e:
        print(f"Greška pri učitavanju weekend planova: {e}")
        import pandas as pd
        return pd.DataFrame()


def delete_weekend_trade_plan(plan_id):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM weekend_trade_plans WHERE id = ?", (int(plan_id),))
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        print(f"Greška pri brisanju weekend plana: {e}")
        return False


def weekend_plan_exists(week_start, instrument):
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT id
                FROM weekend_trade_plans
                WHERE week_start = ? AND instrument = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(week_start), str(instrument)),
            ).fetchone()
        return bool(row)
    except Exception as e:
        print(f"Greška pri proveri postojanja weekend plana: {e}")
        return False


def get_weekend_trade_plan_row(week_start, instrument):
    """Jedan zapis za par nedelja+instrument (najnoviji id), ili None."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, week_start, instrument, bias, setup_type, key_levels,
                       tv_chart_url, invalidation, entry_idea, risk_plan, execution_notes, status, user_note,
                       last_update_kind, last_update_note, last_update_tv_url
                FROM weekend_trade_plans
                WHERE week_start = ? AND instrument = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (str(week_start), str(instrument)),
            ).fetchone()
        if not row:
            return None
        cols = [
            "id", "created_at", "week_start", "instrument", "bias", "setup_type", "key_levels",
            "tv_chart_url", "invalidation", "entry_idea", "risk_plan", "execution_notes", "status", "user_note",
            "last_update_kind", "last_update_note", "last_update_tv_url",
        ]
        return dict(zip(cols, row))
    except Exception as e:
        print(f"Greška pri učitavanju weekend plan reda: {e}")
        return None


def get_weekend_trade_plan_by_id(plan_id):
    """Jedan zapis po id (za Telegram posle WIN/LOSE/BE)."""
    try:
        with get_connection() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, week_start, instrument, bias, setup_type, key_levels,
                       tv_chart_url, invalidation, entry_idea, risk_plan, execution_notes, status, user_note,
                       last_update_kind, last_update_note, last_update_tv_url
                FROM weekend_trade_plans
                WHERE id = ?
                LIMIT 1
                """,
                (int(plan_id),),
            ).fetchone()
        if not row:
            return None
        cols = [
            "id", "created_at", "week_start", "instrument", "bias", "setup_type", "key_levels",
            "tv_chart_url", "invalidation", "entry_idea", "risk_plan", "execution_notes", "status", "user_note",
            "last_update_kind", "last_update_note", "last_update_tv_url",
        ]
        return dict(zip(cols, row))
    except Exception as e:
        print(f"Greška pri učitavanju weekend plana po id: {e}")
        return None


def update_weekend_trade_plan_user_note(plan_id, user_note):
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            st_row = cur.execute(
                "SELECT status FROM weekend_trade_plans WHERE id = ?",
                (int(plan_id),),
            ).fetchone()
            if st_row and str(st_row[0]).upper().strip() in ("WIN", "LOSE", "BE"):
                return False
            cur.execute(
                "UPDATE weekend_trade_plans SET user_note = ? WHERE id = ?",
                (str(user_note or ""), int(plan_id)),
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        print(f"Greška pri ažuriranju user_note weekend plana: {e}")
        return False


def update_weekend_trade_plan_status(plan_id, status):
    try:
        allowed = {"ACTIVE", "WIN", "LOSE", "BE"}
        status_val = str(status).upper().strip()
        if status_val not in allowed:
            status_val = "ACTIVE"
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE weekend_trade_plans SET status = ? WHERE id = ?",
                (status_val, int(plan_id)),
            )
            conn.commit()
            return cur.rowcount > 0
    except Exception as e:
        print(f"Greška pri ažuriranju statusa weekend plana: {e}")
        return False


def get_recent_ai_signals(instruments, limit_rows=20):
    try:
        items = [str(x).strip() for x in (instruments or []) if str(x).strip()]
        if not items:
            import pandas as pd
            return pd.DataFrame(columns=["timestamp", "instrument", "predicted_bias", "master_score", "status", "ai_note"])
        placeholders = ",".join(["?"] * len(items))
        q = f"""
            SELECT timestamp, instrument, predicted_bias, master_score, status, ai_note
            FROM signal_tracker
            WHERE instrument IN ({placeholders})
            ORDER BY timestamp DESC
            LIMIT ?
        """
        with get_connection() as conn:
            rows = conn.execute(q, (*items, int(limit_rows))).fetchall()
        import pandas as pd
        return pd.DataFrame(
            rows,
            columns=["timestamp", "instrument", "predicted_bias", "master_score", "status", "ai_note"],
        )
    except Exception as e:
        print(f"Greška pri učitavanju AI signala iz baze: {e}")
        import pandas as pd
        return pd.DataFrame(columns=["timestamp", "instrument", "predicted_bias", "master_score", "status", "ai_note"])

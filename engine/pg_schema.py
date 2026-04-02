"""PostgreSQL DDL for Supabase (core app tables)."""

from __future__ import annotations

PG_STATEMENTS: list[str] = [
    """
    CREATE TABLE IF NOT EXISTS signal_tracker (
        id BIGSERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ,
        instrument TEXT,
        entry_price DOUBLE PRECISION,
        predicted_bias TEXT,
        master_score DOUBLE PRECISION,
        status TEXT DEFAULT 'OPEN',
        result_price DOUBLE PRECISION,
        win_loss TEXT,
        ai_note TEXT,
        is_verified INTEGER DEFAULT 0,
        mfe_pct DOUBLE PRECISION,
        mae_pct DOUBLE PRECISION,
        efficiency_ratio DOUBLE PRECISION,
        adjusted_win INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_signal_tracker_instrument_ts ON signal_tracker(instrument, timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_signal_tracker_status_ts ON signal_tracker(status, timestamp)",
    """
    CREATE TABLE IF NOT EXISTS system_performance_ledger (
        id BIGSERIAL PRIMARY KEY,
        signal_tracker_id BIGINT REFERENCES signal_tracker(id),
        created_at TIMESTAMPTZ,
        instrument TEXT,
        signal_type TEXT,
        master_score DOUBLE PRECISION,
        predicted_bias TEXT,
        p_entry DOUBLE PRECISION,
        regime_snapshot TEXT,
        data_quality_overall DOUBLE PRECISION,
        horizon_end_4w TEXT,
        p_max_4w DOUBLE PRECISION,
        p_min_4w DOUBLE PRECISION,
        p_close_4w DOUBLE PRECISION,
        return_4w_pct DOUBLE PRECISION,
        mfe_pct DOUBLE PRECISION,
        mae_pct DOUBLE PRECISION,
        directional_win INTEGER,
        status TEXT DEFAULT 'PENDING',
        backfill_source TEXT,
        efficiency_ratio DOUBLE PRECISION,
        adjusted_win INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_perf_ledger_status_horizon ON system_performance_ledger(status, horizon_end_4w)",
    "CREATE INDEX IF NOT EXISTS idx_perf_ledger_signal_id ON system_performance_ledger(signal_tracker_id)",
    """
    CREATE TABLE IF NOT EXISTS swing_sweep_runs (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ,
        run_key TEXT,
        scenario TEXT,
        app_bias TEXT,
        sweep_limit INTEGER,
        use_prefilter INTEGER,
        notes TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS swing_sweep_results (
        id BIGSERIAL PRIMARY KEY,
        run_id BIGINT REFERENCES swing_sweep_runs(id),
        asset TEXT,
        bias TEXT,
        confidence DOUBLE PRECISION,
        clarity_score DOUBLE PRECISION,
        prefilter_score DOUBLE PRECISION,
        stop_zone TEXT,
        target_zone TEXT,
        plan_note TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS weekend_trade_plans (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ,
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
        status TEXT DEFAULT 'ACTIVE',
        user_note TEXT,
        last_update_kind TEXT,
        last_update_note TEXT,
        last_update_tv_url TEXT
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_weekend_plan_week_instrument ON weekend_trade_plans(week_start, instrument)",
    """
    CREATE TABLE IF NOT EXISTS daily_snapshots (
        id BIGSERIAL PRIMARY KEY,
        snapshot_at TEXT NOT NULL,
        instrument_kind TEXT NOT NULL,
        instrument_code TEXT NOT NULL,
        master_score DOUBLE PRECISION,
        verdict_or_signal TEXT,
        spec_z DOUBLE PRECISION,
        z_spread DOUBLE PRECISION,
        scenario TEXT,
        shock_type TEXT,
        lookback INTEGER,
        panel_json TEXT,
        score_24h_reference DOUBLE PRECISION,
        score_decomposition_json TEXT,
        UNIQUE(snapshot_at, instrument_kind, instrument_code)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_daily_snapshots_at ON daily_snapshots(snapshot_at)",
    "CREATE INDEX IF NOT EXISTS idx_daily_snapshots_code ON daily_snapshots(instrument_code, snapshot_at)",
    """
    CREATE TABLE IF NOT EXISTS eia_cache (
        series_key TEXT,
        date TEXT,
        value DOUBLE PRECISION,
        PRIMARY KEY (series_key, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS eia_meta (
        series_key TEXT PRIMARY KEY,
        last_fetched TEXT
    )
    """,
]


def ensure_postgres_schema() -> None:
    from .db_backend import get_connection, use_postgresql

    if not use_postgresql():
        return
    with get_connection() as conn:
        cur = conn.cursor()
        for stmt in PG_STATEMENTS:
            cur.execute(stmt.strip())
        conn.commit()


def postgres_add_missing_columns() -> None:
    from .db_backend import get_connection, use_postgresql

    if not use_postgresql():
        return
    alters = [
        "ALTER TABLE signal_tracker ADD COLUMN IF NOT EXISTS mfe_pct DOUBLE PRECISION",
        "ALTER TABLE signal_tracker ADD COLUMN IF NOT EXISTS mae_pct DOUBLE PRECISION",
        "ALTER TABLE signal_tracker ADD COLUMN IF NOT EXISTS efficiency_ratio DOUBLE PRECISION",
        "ALTER TABLE signal_tracker ADD COLUMN IF NOT EXISTS adjusted_win INTEGER",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS mfe_pct DOUBLE PRECISION",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS mae_pct DOUBLE PRECISION",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS efficiency_ratio DOUBLE PRECISION",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS adjusted_win INTEGER",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS directional_win INTEGER",
        "ALTER TABLE system_performance_ledger ADD COLUMN IF NOT EXISTS backfill_source TEXT",
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        for sql in alters:
            try:
                cur.execute(sql)
            except Exception:
                pass
        conn.commit()

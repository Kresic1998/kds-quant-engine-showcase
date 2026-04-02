import sqlite3

from engine.performance_ledger_engine import (
    _compute_excursions,
    _score_bucket,
    filter_filled_ledger_for_explorer,
    ledger_status_counts,
    win_rate_summary_for_df,
)


def test_compute_excursions_long_win():
    ret, mfe, mae, er, win, adj = _compute_excursions(100.0, 98.0, 105.0, 103.0, "LONG")
    assert win == 1 and adj == 1
    assert ret == 3.0
    assert mfe == 5.0
    assert mae == 2.0
    assert abs(er - mfe / max(mae, 0.001)) < 1e-9


def test_compute_excursions_short():
    ret, mfe, mae, er, win, adj = _compute_excursions(100.0, 95.0, 102.0, 97.0, "SHORT")
    assert win == 1 and adj == 1
    assert abs(ret - 3.0) < 1e-9


def test_score_bucket_edges():
    assert "[6-8)" == _score_bucket(6.0)
    assert "[8-10]" == _score_bucket(10.0)


def test_ledger_status_counts_minimal_table(tmp_path, monkeypatch):
    db = str(tmp_path / "led.db")
    monkeypatch.setattr("engine.config.get_sqlite_db_path", lambda: db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE system_performance_ledger (
                id INTEGER PRIMARY KEY,
                status TEXT,
                backfill_source TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO system_performance_ledger (status, backfill_source) VALUES ('FILLED', 'weekly_friday_104w')"
        )
        conn.execute("INSERT INTO system_performance_ledger (status, backfill_source) VALUES ('NO_DATA', 'weekly_friday_104w')")
        conn.commit()
    r = ledger_status_counts()
    assert r["table_exists"] is True
    assert r["by_status"]["FILLED"] == 1
    assert r["by_status"]["NO_DATA"] == 1
    assert r["backfill_rows"] == 2


def test_filter_filled_ledger_weeks_and_instrument():
    import pandas as pd
    from datetime import datetime, timedelta, timezone

    snap = '{"shock_type":"NONE","curve_signal":"NORMAL","shield_active":false,"dxy_trend":"NEUTRAL","scenario":"Normalno stanje"}'
    now = datetime.now(timezone.utc)
    ts_old = now - timedelta(days=30)
    ts_mid = now - timedelta(days=10)
    ts_new = now - timedelta(days=3)
    df = pd.DataFrame(
        {
            "instrument": ["EUR", "GBP", "EUR"],
            "directional_win": [1, 0, 1],
            "return_4w_pct": [1.0, -1.0, 2.0],
            "mae_pct": [1.0, 1.0, 1.0],
            "mfe_pct": [1.0, 1.0, 1.0],
            "master_score": [5.0, 5.0, 8.0],
            "regime_snapshot": [snap, snap, snap],
            "created_at": [ts_old, ts_mid, ts_new],
        }
    )
    sub = filter_filled_ledger_for_explorer(df, weeks_back=2, instrument="EUR")
    assert len(sub) == 1
    summ = win_rate_summary_for_df(sub)
    assert summ["n"] == 1


def test_ledger_status_counts_without_backfill_column(tmp_path, monkeypatch):
    """Older DBs: no backfill_source column — do not crash."""
    db = str(tmp_path / "legacy.db")
    monkeypatch.setattr("engine.config.get_sqlite_db_path", lambda: db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE system_performance_ledger (id INTEGER PRIMARY KEY, status TEXT)"
        )
        conn.execute("INSERT INTO system_performance_ledger (status) VALUES ('FILLED')")
        conn.commit()
    r = ledger_status_counts()
    assert r["table_exists"] is True
    assert r["backfill_rows"] == 0

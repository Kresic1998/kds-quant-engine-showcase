import sqlite3

from engine import audit_engine


def test_sync_win_loss_from_adjusted_aligns(tmp_path, monkeypatch):
    db = str(tmp_path / "st.db")
    monkeypatch.setattr("engine.config.get_sqlite_db_path", lambda: db)
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE signal_tracker (
                id INTEGER PRIMARY KEY,
                instrument TEXT,
                status TEXT,
                predicted_bias TEXT,
                win_loss TEXT,
                adjusted_win INTEGER
            )
            """
        )
        conn.execute(
            "INSERT INTO signal_tracker VALUES (1,'EUR','CLOSED','LONG BIAS','LOSS',1)"
        )
        conn.execute(
            "INSERT INTO signal_tracker VALUES (2,'EUR','CLOSED','NEUTRAL','WIN',NULL)"
        )
        conn.commit()
    n, _ = audit_engine.sync_win_loss_from_adjusted_signal_tracker()
    assert n == 2
    mism, meta = audit_engine.verify_signal_tracker_win_alignment()
    assert meta.get("mismatch_count", 1) == 0
    assert not mism
    with sqlite3.connect(db) as conn:
        r1 = conn.execute("SELECT win_loss FROM signal_tracker WHERE id=1").fetchone()[0]
        r2 = conn.execute("SELECT win_loss FROM signal_tracker WHERE id=2").fetchone()[0]
    assert r1 == "WIN"
    assert r2 == "NEUTRAL"

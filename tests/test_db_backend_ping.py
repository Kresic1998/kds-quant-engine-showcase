import sqlite3

from engine import db_backend


def test_ping_sqlite_ok(tmp_path, monkeypatch):
    db = str(tmp_path / "p.db")
    monkeypatch.setattr("engine.config.get_sqlite_db_path", lambda: db)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    db_backend.reset_db_backend_cache()
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t(x INT)")
        c.commit()
    ok, msg = db_backend.ping_database()
    db_backend.reset_db_backend_cache()
    assert ok is True
    assert msg == "ok"

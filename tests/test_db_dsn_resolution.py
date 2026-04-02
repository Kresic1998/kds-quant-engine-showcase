"""DSN discovery for Supabase (multiple env + secrets shapes)."""
from __future__ import annotations

from engine import db_backend


def test_strip_secret_quotes():
    assert db_backend._strip_secret('"postgresql://x"') == "postgresql://x"
    assert db_backend._strip_secret("' postgresql://y '") == "postgresql://y"


def test_read_dsn_from_supabase_postgres_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_DB_URL", raising=False)
    monkeypatch.setenv(
        "SUPABASE_POSTGRES_URL",
        "postgresql://user:pass@host:6543/postgres",
    )
    db_backend.reset_db_backend_cache()
    try:
        assert db_backend._read_dsn().startswith("postgresql://")
        assert db_backend.use_postgresql() is True
    finally:
        monkeypatch.delenv("SUPABASE_POSTGRES_URL", raising=False)
        db_backend.reset_db_backend_cache()

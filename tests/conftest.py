"""Tests always use SQLite file backend (ignore host DATABASE_URL)."""
from __future__ import annotations

import os

import pytest

# Ad-hoc email scripts moved from root; not real pytest tests (manual send helpers).
collect_ignore = [
    "test_email.py",
    "test_email_send.py",
    "test_resend_email.py",
]


@pytest.fixture(autouse=True)
def _tests_use_sqlite_not_supabase():
    os.environ.pop("DATABASE_URL", None)
    os.environ.pop("SUPABASE_DB_URL", None)
    os.environ.pop("SUPABASE_POSTGRES_URL", None)
    import engine.config as ec

    ec.reset_sqlite_db_path_cache()
    vars(ec).pop("DB_NAME", None)
    from engine.db_backend import reset_db_backend_cache

    reset_db_backend_cache()
    yield
    ec.reset_sqlite_db_path_cache()
    vars(ec).pop("DB_NAME", None)
    reset_db_backend_cache()

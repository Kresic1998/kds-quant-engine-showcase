"""Seed copy when DB path is empty (Streamlit persistent volume first boot)."""

from pathlib import Path

from engine import db_engine


def test_ensure_sqlite_copies_repo_seed_to_new_path(tmp_path, monkeypatch):
    target = tmp_path / "persistent.db"
    monkeypatch.setattr("engine.config.get_sqlite_db_path", lambda: str(target))
    assert not target.exists()
    db_engine.ensure_sqlite_database_file_ready()
    assert target.exists()
    assert target.stat().st_size > 0


def test_is_ephemeral_mount_path():
    from engine.config import is_default_ephemeral_sqlite_path

    assert is_default_ephemeral_sqlite_path("/mount/src/foo/cot_quant_master.db") is True
    assert is_default_ephemeral_sqlite_path("/data/app/cot_quant_master.db") is False

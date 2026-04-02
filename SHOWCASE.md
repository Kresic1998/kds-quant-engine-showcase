# Showcase slice (read me first)

This folder is a **curated subset** of the full KDS Quant stack: documentation, CI/CD, data/DB infrastructure, **data integrity gates**, **persistence**, **audit/ledger** plumbing, and representative tests. **Scoring, shock, swing, and related “alpha” engine modules are intentionally omitted.** There is **no** bundled UI in this repository—the focus is a **headless Quant Engine** slice.

## Scope

- **In scope here:** `engine/` modules for DB routing, schema helpers, **data quality** and **hard data** gates, retry HTTP, CFTC constants, audit + performance ledger helpers, `cftc_loader.py`, migration script, tests, and CI.
- **Out of scope:** Proprietary scoring, notify pipelines, and any Streamlit or other legacy desktop-style UI. Interactive product UI is **UI implementation handled by the decoupled Next.js frontend** (private).

## What was added beyond the original file list

These files exist only so copied entry points **import successfully** and tests can run:

| File | Why |
|------|-----|
| `engine/config.py` | DB path, ticker/display maps — used by `db_engine`, `audit_engine`, `performance_ledger_engine`, `cftc_loader`. |
| `engine/audit_metrics.py` | Excursion / win logic shared by audit + ledger. |
| `engine/retry_util.py` | Backoff helper used by `retry_http` and `yahoo_single_history`. |
| `engine/cot_cftc_constants.py` | CFTC instrument lists for `cftc_loader`. |
| `engine/yahoo_single_history.py` | Price history helper used by `cftc_loader`. |

## Runtime expectations

- **No `app.py`:** This showcase does not ship a runnable web entrypoint. Use **`python cftc_loader.py`** / **`python scripts/migrate_sqlite_to_supabase.py`** with the included `engine/` slice and environment variables as in the main repo docs.
- **`cot_quant_master.db`** is included so `tests/test_db_bootstrap.py` can verify seed-copy behavior (~5 MB).
- **`pytest tests/`** is the primary way to validate **gates, persistence, and audit** helpers in CI and locally.

## Security

Do not add `.streamlit/secrets.toml` or production credentials to this tree. Use env vars or a private secrets file outside git, as in `SECURITY.md`.

## GitHub Actions

- **`ci.yml`** is fully active (pytest + engine coverage, fail-under **30%**).
- **`heartbeat.yml`**, **`quant-notify-*.yml`**, **`perf-backfill-artifact.yml`** are **manual-only placeholders** that explain missing entrypoints; they avoid scheduled failures on this slice. Copy the real workflows from your private monorepo when you fork with the full tree.

## Blueprint

`QUANT_SYSTEM_BLUEPRINT.md` keeps the **full-system module map** for context; numeric calibration in §§2–4 uses **`[PARAM]`** placeholders in this public copy.

## Tests

From this directory (with venv + `pip install -r requirements.txt`):

```bash
pytest tests/
```

`tests/test_data_quality_gate.py` in this slice **omits** two tests that imported `score_engine` (full repo only).

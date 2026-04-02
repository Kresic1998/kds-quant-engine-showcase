# Contributing

## Environment

- Python **3.11+** (see `requirements.txt`).
- Create a virtual environment, install deps: `pip install -r requirements.txt`.
- For database URLs and local paths, use environment variables per `SECURITY.md` and `engine/config.py`. The `.streamlit/secrets.example.toml` file is a legacy DSN template only; never commit real secrets.

## Tests

```bash
pytest
```

CI runs the same suite (see `.github/workflows/ci.yml`). Prefer small, focused tests next to related modules under `tests/`.

## Pull requests

- Keep changes scoped and documented in the PR description.
- Match existing style and naming in `engine/` and `tests/`.
- Do not commit secrets, large binary data, or personal telemetry snapshots.

## License

By contributing, you agree that your contributions are licensed under the same terms as the project (see `LICENSE`).

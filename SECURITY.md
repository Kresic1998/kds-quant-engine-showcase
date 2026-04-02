# Security policy

## Supported versions

Security fixes are applied on the default branch (`main`). Use the latest commit for production-style deployments.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for undisclosed security problems.

- Email the maintainers with a clear description, affected component, and reproduction steps if possible.
- Allow reasonable time for triage and a fix before public disclosure.

## Secrets and credentials

- Never commit `.streamlit/secrets.toml`, database passwords, or API keys.
- Use `.streamlit/secrets.example.toml` as a template only; copy to `secrets.toml` locally.
- If credentials were ever exposed in git history, rotate them at the provider (e.g. Supabase dashboard) and consider history cleanup.

## Scope

This repository is a quantitative tooling stack; it is not a hosted service. Reports should relate to this codebase (e.g. accidental secret leakage patterns, unsafe defaults in documented setup).

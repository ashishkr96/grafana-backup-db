# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

## [1.0.0] - 2026-02-21

### Added
- Schema-agnostic MySQL and SQLite backup support
- Auto-discovery of all tables via `SHOW TABLES` at runtime
- Row-level JSON export with configurable batch size (no OOM)
- Compressed `.tar.gz` output with timestamped archive names
- `manifest.json` in every archive with row counts, timestamps, and status
- Multi-database support — back up multiple DBs in a single run
- Config via `config.yaml`, environment variables, or CLI flags
- CLI flags: `--config`, `--db`, `--output`, `--no-compress`, `--batch-size`
- Environment variable interpolation in `config.yaml` (e.g. `${VAR}`)
- MIT License
- Branch protection rule: PRs required to merge into `main`

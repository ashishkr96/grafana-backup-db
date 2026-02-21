# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt pytest flake8

# Run all tests
pytest test_backup.py -v

# Run a single test class or test
pytest test_backup.py::TestExportTable -v
pytest test_backup.py::TestExportTable::test_batching_fetches_all_rows -v

# Lint
flake8 backup.py --max-line-length=120

# Run backup (SQLite, no config file needed)
SQLITE_PATH=/var/lib/grafana/grafana.db python backup.py

# Run backup (MySQL, no config file needed)
MYSQL_HOST=127.0.0.1 MYSQL_USER=root MYSQL_PASSWORD=secret MYSQL_DATABASE=grafana python backup.py

# Run backup using config.yaml
cp .env.example .env   # fill in secrets
export $(grep -v '^#' .env | xargs)
python backup.py
```

## Architecture

The entire tool lives in a single file: `backup.py`. `test_backup.py` contains all tests.

**Connector abstraction** (`backup.py:53–137`): `SQLiteConnector` and `MySQLConnector` share a common interface — `get_tables()`, `get_row_count(table)`, `fetch_batch(table, limit, offset)`, `close()`. `make_connector(cfg)` is the factory that selects the right one based on `cfg["type"]`. SQLite is opened read-only via URI (`file:...?mode=ro`) to prevent accidental writes to the live database.

**Config loading** (`backup.py:143–239`): `load_config()` reads `config.yaml` and recursively interpolates `${ENV_VAR}` placeholders via `_interpolate_config()`. `build_db_configs()` merges all sources into a final list of DB config dicts, following this priority: CLI args > environment variables > config.yaml > defaults. When no `databases:` entries exist in the config, it falls back to `SQLITE_PATH` / `MYSQL_HOST` env vars.

**Backup flow** (`backup.py:244–383`): `backup_database()` orchestrates one DB: creates a timestamped run directory, iterates tables, calls `export_table()` per table, writes `manifest.json`, then optionally compresses the directory to `.tar.gz` and removes the raw directory. Each row is written as an individual JSON file. Filenames are derived from columns `title → name → slug → login → email → uid` (via `_row_stem()`), with the row index prepended to guarantee uniqueness.

**Output structure**: All archives land under `output_dir` (default `./backups`). The folder/archive name is solely the date/time string from `filename_format` (default `%d-%m-%Y`) — it does not include the DB label.

## PR Requirements

Every PR to `main` must:
1. Update `CHANGELOG.md` (enforced by CI)
2. Pass `flake8 backup.py --max-line-length=120`
3. Pass `pytest test_backup.py -v`

New connectors must implement the same four-method interface as `SQLiteConnector` and `MySQLConnector` and be registered in `make_connector()`.

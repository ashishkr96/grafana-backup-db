#!/usr/bin/env python3
"""
Database Backup Tool
--------------------
Schema-agnostic backup for MySQL and SQLite databases.
Auto-discovers all tables, exports every row as JSON, compresses to .tar.gz.

Supports:
  - SQLite  (Grafana default — no extra dependencies)
  - MySQL / MariaDB

Config priority (highest → lowest):
  1. CLI arguments
  2. Environment variables
  3. config.yaml (or --config path)
  4. Built-in defaults

Usage:
  python backup.py                          # uses config.yaml + env vars
  python backup.py --config prod.yaml       # custom config file
  python backup.py --db grafana             # backup only specific DB label
  python backup.py --output /mnt/backups    # override output directory

  # SQLite via env (no config.yaml needed)
  SQLITE_PATH=/var/lib/grafana/grafana.db python backup.py

  # MySQL via env (no config.yaml needed)
  MYSQL_HOST=127.0.0.1 MYSQL_USER=root MYSQL_PASSWORD=secret MYSQL_DATABASE=grafana python backup.py
"""

import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
from datetime import datetime
from pathlib import Path

try:
    import yaml
except ImportError:
    sys.exit("❌ PyYAML not installed. Run: pip install pyyaml")


# ---------------------------------------------------------------------------
# Connector abstraction
# Each connector exposes: get_tables(), get_row_count(table), fetch_batch(table, limit, offset), close()
# ---------------------------------------------------------------------------

class SQLiteConnector:
    """Read-only connector for SQLite databases (Grafana's default storage)."""

    def __init__(self, cfg: dict):
        path = Path(cfg.get("path", "").strip()).expanduser().resolve()
        if not str(path):
            raise ValueError("SQLite config requires a 'path' field pointing to the .db file.")
        if not path.exists():
            raise FileNotFoundError(f"SQLite file not found: {path}")
        # Open read-only so we never accidentally mutate the live DB
        self.conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row

    def get_tables(self) -> list[str]:
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return [row[0] for row in cur.fetchall()]

    def get_row_count(self, table: str) -> int:
        cur = self.conn.execute(f'SELECT COUNT(*) FROM "{table}"')
        return cur.fetchone()[0]

    def fetch_batch(self, table: str, limit: int, offset: int) -> list[dict]:
        cur = self.conn.execute(
            f'SELECT * FROM "{table}" LIMIT ? OFFSET ?', (limit, offset)
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self):
        self.conn.close()


class MySQLConnector:
    """Connector for MySQL / MariaDB databases."""

    def __init__(self, cfg: dict):
        try:
            import mysql.connector
        except ImportError:
            sys.exit(
                "❌ mysql-connector-python not installed.\n"
                "   Run: pip install mysql-connector-python"
            )
        self.conn = mysql.connector.connect(
            host=cfg["host"],
            port=int(cfg.get("port", 3306)),
            user=cfg["user"],
            password=cfg["password"],
            database=cfg["database"],
            connection_timeout=10,
        )
        self.cursor = self.conn.cursor(dictionary=True)

    def get_tables(self) -> list[str]:
        self.cursor.execute("SHOW TABLES")
        return [list(row.values())[0] for row in self.cursor.fetchall()]

    def get_row_count(self, table: str) -> int:
        self.cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
        return self.cursor.fetchone()["cnt"]

    def fetch_batch(self, table: str, limit: int, offset: int) -> list[dict]:
        self.cursor.execute(
            f"SELECT * FROM `{table}` LIMIT %s OFFSET %s", (limit, offset)
        )
        return self.cursor.fetchall()

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            pass


def make_connector(cfg: dict):
    """Factory — return the right connector based on cfg['type']."""
    db_type = cfg.get("type", "mysql").lower()
    if db_type == "sqlite":
        return SQLiteConnector(cfg)
    if db_type in ("mysql", "mariadb"):
        return MySQLConnector(cfg)
    raise ValueError(f"Unknown database type '{db_type}'. Use 'sqlite' or 'mysql'.")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate_env(value):
    """Replace ${VAR} placeholders in string values with environment variable values."""
    if not isinstance(value, str):
        return value
    def replacer(match):
        var = match.group(1)
        result = os.getenv(var)
        if result is None:
            raise ValueError(f"Config references undefined environment variable: ${{{var}}}")
        return result
    return ENV_VAR_PATTERN.sub(replacer, value)


def _interpolate_config(obj):
    """Recursively walk config and interpolate env vars."""
    if isinstance(obj, dict):
        return {k: _interpolate_config(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_config(i) for i in obj]
    return _interpolate_env(obj)


def load_config(config_path: str | None) -> dict:
    path = Path(config_path or "config.yaml")
    if not path.exists():
        return {}
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    return _interpolate_config(raw)


def build_db_configs(file_cfg: dict, cli_args: argparse.Namespace) -> list[dict]:
    """
    Merge all config sources into a final list of DB configs.
    Each entry has at minimum: name, type, batch_size
    SQLite entries add:  path
    MySQL entries add:   host, port, user, password, database
    """
    databases = list(file_cfg.get("databases", []))

    # Env var fallback when no config file databases are defined
    if not databases:
        sqlite_path = os.getenv("SQLITE_PATH")
        if sqlite_path:
            databases.append({
                "name": os.getenv("SQLITE_NAME", "sqlite-default"),
                "type": "sqlite",
                "path": sqlite_path,
            })

        mysql_host = os.getenv("MYSQL_HOST")
        if mysql_host:
            databases.append({
                "name": os.getenv("MYSQL_NAME", "mysql-default"),
                "type": "mysql",
                "host": mysql_host,
                "port": int(os.getenv("MYSQL_PORT", "3306")),
                "user": os.getenv("MYSQL_USER", ""),
                "password": os.getenv("MYSQL_PASSWORD", ""),
                "database": os.getenv("MYSQL_DATABASE", ""),
            })

    if not databases:
        sys.exit(
            "❌ No database configuration found.\n"
            "\n"
            "Options:\n"
            "  • Add databases to config.yaml\n"
            "  • SQLite: set SQLITE_PATH=/path/to/grafana.db\n"
            "  • MySQL:  set MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE"
        )

    backup_defaults = file_cfg.get("backup", {})
    default_batch = int(backup_defaults.get("batch_size", 1000))

    normalized = []
    for db in databases:
        entry = dict(db)
        # Sensible name default
        entry.setdefault("name", db.get("database") or db.get("path") or "unnamed")
        entry.setdefault("type", "mysql")
        entry["batch_size"] = int(db.get("batch_size", default_batch))
        normalized.append(entry)

    # Filter by --db if specified
    if cli_args.db:
        selected = [d for d in normalized if d["name"] in cli_args.db]
        if not selected:
            available = [d["name"] for d in normalized]
            sys.exit(f"❌ No DB matching --db {cli_args.db}. Available: {available}")
        return selected

    return normalized


# ---------------------------------------------------------------------------
# Backup logic
# ---------------------------------------------------------------------------

# Columns tried in order when naming a row's JSON file.
# The first non-empty value found is used as the filename stem.
_NAME_COLUMNS = ("title", "name", "slug", "login", "email", "uid")


def _safe_filename(value: str) -> str:
    """Sanitize a value for use as a filename (no path separators, no spaces)."""
    value = str(value).strip()
    value = re.sub(r"[^\w\-.]", "_", value)   # keep word chars, dash, dot
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unnamed"


def _row_stem(row: dict, index: int) -> str:
    """Return a human-readable (and unique) filename stem for this row.

    The index is always prepended (e.g. ``3_My_Dashboard``) so that two rows
    with identical titles never produce the same filename.
    """
    for col in _NAME_COLUMNS:
        val = row.get(col)
        if val and str(val).strip():
            return f"{index}_{_safe_filename(str(val))}"
    return f"row_{index}"


def export_table(connector, table: str, table_dir: Path, batch_size: int) -> int:
    """
    Dump every row of `table` as an individual JSON file under table_dir.
    Files are named after the row's title/name/slug when available,
    otherwise row_{n}.json.  Uses LIMIT/OFFSET batching.
    Returns total rows exported.
    """
    table_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    offset = 0

    while True:
        rows = connector.fetch_batch(table, batch_size, offset)
        if not rows:
            break
        for row in rows:
            stem = _row_stem(row, total)
            with open(table_dir / f"{stem}.json", "w") as f:
                json.dump(row, f, indent=2, default=str)
            total += 1
        offset += batch_size
        if len(rows) < batch_size:
            break  # last (partial) batch — done

    return total


def backup_database(db_cfg: dict, output_root: Path, compress: bool, filename_format: str) -> dict:
    """Run a full backup for one database entry. Returns manifest dict."""
    label    = db_cfg["name"]
    db_type  = db_cfg.get("type", "mysql")
    time_str = datetime.now().strftime(filename_format)
    run_dir  = output_root / time_str
    run_dir.mkdir(parents=True, exist_ok=True)

    conn_desc = (
        db_cfg.get("path", "")
        if db_type == "sqlite"
        else f"{db_cfg.get('database')}@{db_cfg.get('host')}:{db_cfg.get('port', 3306)}"
    )

    manifest = {
        "database_label": label,
        "type":           db_type,
        "connection":     conn_desc,
        "started_at":     datetime.now().isoformat(),
        "tables":         {},
        "status":         "in_progress",
    }

    print(f"\n{'─'*60}")
    print(f"  Backing up : {label}  [{db_type}]")
    print(f"  Connection : {conn_desc}")
    print(f"{'─'*60}")

    try:
        connector = make_connector(db_cfg)
    except Exception as e:
        manifest["status"] = "connection_failed"
        manifest["error"]  = str(e)
        print(f"  ❌ Connection failed: {e}")
        _write_manifest(run_dir, manifest)
        return manifest

    try:
        tables = connector.get_tables()
        print(f"  Found {len(tables)} table(s): {', '.join(tables)}\n")

        for table in tables:
            row_count = connector.get_row_count(table)
            print(f"  → {table:<40} {row_count:>6} row(s)", end="", flush=True)
            exported = export_table(connector, table, run_dir / table, db_cfg["batch_size"])
            manifest["tables"][table] = {"rows": exported}
            print("  ✓")

        manifest["status"]        = "success"
        manifest["completed_at"]  = datetime.now().isoformat()
        manifest["total_tables"]  = len(tables)
        manifest["total_rows"]    = sum(t["rows"] for t in manifest["tables"].values())

    except Exception as e:
        manifest["status"] = "export_failed"
        manifest["error"]  = str(e)
        print(f"\n  ❌ Export failed: {e}")
    finally:
        connector.close()

    _write_manifest(run_dir, manifest)

    if compress and manifest["status"] == "success":
        archive = _compress(run_dir)
        manifest["archive"] = str(archive)
        print(f"\n  📦 Archive : {archive}")
    else:
        manifest["backup_dir"] = str(run_dir)
        print(f"\n  📁 Directory: {run_dir}")

    return manifest


def _write_manifest(run_dir: Path, manifest: dict):
    with open(run_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)


def _compress(source_dir: Path) -> Path:
    archive = Path(str(source_dir) + ".tar.gz")
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(source_dir, arcname=source_dir.name)
    shutil.rmtree(source_dir)
    return archive


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Schema-agnostic backup for MySQL and SQLite. Every table, every row → JSON.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python backup.py                           # backup all DBs in config.yaml
  python backup.py --db grafana-sqlite       # backup only one entry by label
  python backup.py --db grafana-local prod   # backup two specific entries
  python backup.py --output /mnt/backups     # override output path
  python backup.py --no-compress             # keep raw directories
  python backup.py --config /etc/mybackup.yaml

  # SQLite via env (no config.yaml needed)
  SQLITE_PATH=/var/lib/grafana/grafana.db python backup.py

  # MySQL via env (no config.yaml needed)
  MYSQL_HOST=127.0.0.1 MYSQL_USER=root MYSQL_PASSWORD=secret MYSQL_DATABASE=grafana python backup.py
        """,
    )
    p.add_argument("--config",      help="Path to config.yaml (default: ./config.yaml)")
    p.add_argument("--db",          nargs="+", metavar="NAME",
                   help="Backup only these database label(s) from config")
    p.add_argument("--output",      help="Override output directory")
    p.add_argument("--no-compress", action="store_true",
                   help="Keep raw directories, skip .tar.gz compression")
    p.add_argument("--batch-size",  type=int,
                   help="Rows per fetch batch (default: 1000)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    file_cfg   = load_config(args.config)
    backup_cfg = file_cfg.get("backup", {})

    output_root = Path(
        args.output
        or os.getenv("BACKUP_OUTPUT_DIR")
        or backup_cfg.get("output_dir", "./backups")
    )
    output_root.mkdir(parents=True, exist_ok=True)

    compress        = not args.no_compress and backup_cfg.get("compress", True)
    filename_format = backup_cfg.get("filename_format", "%d-%m-%Y")
    db_configs      = build_db_configs(file_cfg, args)

    if args.batch_size:
        for d in db_configs:
            d["batch_size"] = args.batch_size

    print("🗄️  Database Backup Tool")
    print(f"   Output  : {output_root.resolve()}")
    print(f"   DBs     : {[(d['name'], d.get('type', 'mysql')) for d in db_configs]}")
    print(f"   Compress: {compress}")

    results = []
    for db_cfg in db_configs:
        results.append(backup_database(db_cfg, output_root, compress, filename_format))

    print(f"\n{'═'*60}")
    success = sum(1 for r in results if r["status"] == "success")
    failed  = len(results) - success
    print(f"  Done: {success} succeeded, {failed} failed\n")
    for r in results:
        icon   = "✅" if r["status"] == "success" else "❌"
        rows   = r.get("total_rows", "—")
        tables = r.get("total_tables", "—")
        print(f"  {icon} {r['database_label']:<25} type={r['type']:<8} tables={tables}  rows={rows}")
    print(f"{'═'*60}\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

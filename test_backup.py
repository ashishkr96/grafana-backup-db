"""
Tests for backup.py
-------------------
Covers: filename sanitisation, row naming, export_table, backup folder naming,
        date format options, env interpolation, config loading, build_db_configs.

Run with:
    pytest test_backup.py -v
"""

import argparse
import json
import os
import sqlite3
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backup import (
    _interpolate_config,
    _interpolate_env,
    _row_stem,
    _safe_filename,
    backup_database,
    build_db_configs,
    export_table,
    load_config,
    make_connector,
    SQLiteConnector,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeConnector:
    """Minimal connector stub — returns a fixed list of row dicts."""

    def __init__(self, rows: list[dict], batch_size: int = 1000):
        self._rows = rows
        self.closed = False

    def get_tables(self):
        return ["fake_table"]

    def get_row_count(self, table):
        return len(self._rows)

    def fetch_batch(self, table, limit, offset):
        return self._rows[offset : offset + limit]

    def close(self):
        self.closed = True


def _make_args(**kwargs) -> argparse.Namespace:
    defaults = dict(config=None, db=None, output=None, no_compress=False, batch_size=None)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# _safe_filename
# ---------------------------------------------------------------------------

class TestSafeFilename:
    def test_plain_string_unchanged(self):
        assert _safe_filename("my-dashboard") == "my-dashboard"

    def test_spaces_become_underscores(self):
        assert _safe_filename("My Dashboard") == "My_Dashboard"

    def test_slashes_removed(self):
        result = _safe_filename("foo/bar/baz")
        assert "/" not in result

    def test_special_chars_replaced(self):
        result = _safe_filename("hello!@#world")
        assert "!" not in result and "@" not in result and "#" not in result

    def test_leading_trailing_underscores_stripped(self):
        result = _safe_filename("  !!hello!!  ")
        assert not result.startswith("_") and not result.endswith("_")

    def test_empty_string_returns_unnamed(self):
        assert _safe_filename("") == "unnamed"

    def test_only_special_chars_returns_unnamed(self):
        assert _safe_filename("!@#$%") == "unnamed"

    def test_multiple_underscores_collapsed(self):
        result = _safe_filename("a   b")
        assert "__" not in result

    def test_dots_preserved(self):
        assert "." in _safe_filename("v1.2.3")


# ---------------------------------------------------------------------------
# _row_stem
# ---------------------------------------------------------------------------

class TestRowStem:
    def test_uses_title_first(self):
        row = {"title": "My Dashboard", "name": "other", "slug": "slug-val"}
        assert _row_stem(row, 0) == "0_My_Dashboard"

    def test_falls_back_to_name(self):
        row = {"name": "my-org", "slug": "s"}
        assert _row_stem(row, 0) == "0_my-org"

    def test_falls_back_to_slug(self):
        row = {"slug": "cool-slug"}
        assert _row_stem(row, 0) == "0_cool-slug"

    def test_falls_back_to_login(self):
        row = {"login": "admin"}
        assert _row_stem(row, 0) == "0_admin"

    def test_falls_back_to_email(self):
        row = {"email": "ops@example.com"}
        assert _row_stem(row, 0) == "0_ops_example.com"

    def test_falls_back_to_uid(self):
        row = {"uid": "abc123"}
        assert _row_stem(row, 0) == "0_abc123"

    def test_falls_back_to_row_index_when_no_name_col(self):
        row = {"id": 7, "data": "blob"}
        assert _row_stem(row, 7) == "row_7"

    def test_empty_title_tries_next_col(self):
        row = {"title": "", "name": "fallback-name"}
        assert _row_stem(row, 0) == "0_fallback-name"

    def test_whitespace_only_title_tries_next_col(self):
        row = {"title": "   ", "slug": "my-slug"}
        assert _row_stem(row, 0) == "0_my-slug"

    def test_index_makes_same_title_unique(self):
        # Two rows with identical titles get different stems via their index
        assert _row_stem({"title": "My Dashboard"}, 0) == "0_My_Dashboard"
        assert _row_stem({"title": "My Dashboard"}, 1) == "1_My_Dashboard"

    def test_index_prefix_increments(self):
        stems = [_row_stem({"title": "Dash"}, i) for i in range(3)]
        assert stems == ["0_Dash", "1_Dash", "2_Dash"]


# ---------------------------------------------------------------------------
# export_table
# ---------------------------------------------------------------------------

class TestExportTable:
    def test_files_named_by_title(self, tmp_path):
        rows = [
            {"title": "CPU Usage", "value": 1},
            {"title": "Memory", "value": 2},
        ]
        conn = FakeConnector(rows)
        total = export_table(conn, "dashboard", tmp_path, batch_size=1000)

        assert total == 2
        assert (tmp_path / "0_CPU_Usage.json").exists()
        assert (tmp_path / "1_Memory.json").exists()

    def test_falls_back_to_row_n_when_no_name_col(self, tmp_path):
        rows = [{"id": 1, "data": "x"}, {"id": 2, "data": "y"}]
        conn = FakeConnector(rows)
        export_table(conn, "migration_log", tmp_path, batch_size=1000)

        assert (tmp_path / "row_0.json").exists()
        assert (tmp_path / "row_1.json").exists()

    def test_file_contents_are_valid_json(self, tmp_path):
        rows = [{"title": "test-dash", "id": 42}]
        conn = FakeConnector(rows)
        export_table(conn, "dashboard", tmp_path, batch_size=1000)

        data = json.loads((tmp_path / "0_test-dash.json").read_text())
        assert data["id"] == 42
        assert data["title"] == "test-dash"

    def test_duplicate_titles_get_unique_names(self, tmp_path):
        rows = [
            {"title": "Duplicate"},
            {"title": "Duplicate"},
            {"title": "Duplicate"},
        ]
        conn = FakeConnector(rows)
        total = export_table(conn, "dashboard", tmp_path, batch_size=1000)

        assert total == 3
        assert (tmp_path / "0_Duplicate.json").exists()
        assert (tmp_path / "1_Duplicate.json").exists()
        assert (tmp_path / "2_Duplicate.json").exists()

    def test_batching_fetches_all_rows(self, tmp_path):
        rows = [{"title": f"dash-{i}"} for i in range(7)]
        conn = FakeConnector(rows)
        total = export_table(conn, "dashboard", tmp_path, batch_size=3)

        assert total == 7
        assert len(list(tmp_path.glob("*.json"))) == 7

    def test_returns_zero_for_empty_table(self, tmp_path):
        conn = FakeConnector([])
        total = export_table(conn, "empty_table", tmp_path, batch_size=1000)
        assert total == 0
        assert list(tmp_path.glob("*.json")) == []

    def test_creates_table_directory(self, tmp_path):
        table_dir = tmp_path / "new_subdir"
        conn = FakeConnector([{"name": "x"}])
        export_table(conn, "t", table_dir, batch_size=1000)
        assert table_dir.is_dir()


# ---------------------------------------------------------------------------
# backup_database — folder naming
# ---------------------------------------------------------------------------

class TestBackupFolderName:
    def _run(self, tmp_path, fmt, db_path):
        db_cfg = {
            "name": "grafana-sqlite",
            "type": "sqlite",
            "path": str(db_path),
            "batch_size": 1000,
        }
        result = backup_database(db_cfg, tmp_path, compress=False, filename_format=fmt)
        return result

    @pytest.fixture()
    def tiny_db(self, tmp_path):
        """Create a minimal SQLite DB with one table and one titled row."""
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE dashboard (id INTEGER, title TEXT)")
        conn.execute("INSERT INTO dashboard VALUES (1, 'My Dashboard')")
        conn.commit()
        conn.close()
        return db_path

    def test_folder_named_by_date_only_no_label(self, tmp_path, tiny_db):
        """Backup folder must be the date string alone — no DB-label prefix."""
        result = self._run(tmp_path, "%d-%m-%Y", tiny_db)
        backup_dir = Path(result["backup_dir"])
        # Parent must be tmp_path directly (no extra nesting)
        assert backup_dir.parent == tmp_path
        # Name must NOT contain the DB label
        assert "grafana-sqlite" not in backup_dir.name
        assert "grafana_sqlite" not in backup_dir.name

    def test_default_format_is_DD_MM_YYYY(self, tmp_path, tiny_db):
        from datetime import datetime
        expected = datetime.now().strftime("%d-%m-%Y")
        result = self._run(tmp_path, "%d-%m-%Y", tiny_db)
        assert Path(result["backup_dir"]).name == expected

    def test_iso_format_YYYY_MM_DD(self, tmp_path, tiny_db):
        from datetime import datetime
        expected = datetime.now().strftime("%Y-%m-%d")
        result = self._run(tmp_path, "%Y-%m-%d", tiny_db)
        assert Path(result["backup_dir"]).name == expected

    def test_format_with_hour_and_minute(self, tmp_path, tiny_db):
        from datetime import datetime
        expected = datetime.now().strftime("%d-%m-%Y_%H-%M")
        result = self._run(tmp_path, "%d-%m-%Y_%H-%M", tiny_db)
        assert Path(result["backup_dir"]).name == expected

    def test_dashboard_file_named_by_title(self, tmp_path, tiny_db):
        result = self._run(tmp_path, "%d-%m-%Y", tiny_db)
        backup_dir = Path(result["backup_dir"])
        dashboard_files = list((backup_dir / "dashboard").glob("*.json"))
        names = [f.stem for f in dashboard_files]
        assert "0_My_Dashboard" in names
        assert "row_0" not in names

    def test_status_success(self, tmp_path, tiny_db):
        result = self._run(tmp_path, "%d-%m-%Y", tiny_db)
        assert result["status"] == "success"

    def test_compress_creates_tar_gz(self, tmp_path, tiny_db):
        db_cfg = {
            "name": "grafana-sqlite",
            "type": "sqlite",
            "path": str(tiny_db),
            "batch_size": 1000,
        }
        result = backup_database(db_cfg, tmp_path, compress=True, filename_format="%d-%m-%Y")
        archive = Path(result["archive"])
        assert archive.suffix == ".gz"
        assert tarfile.is_tarfile(archive)

    def test_connection_failure_recorded_in_manifest(self, tmp_path):
        db_cfg = {
            "name": "bad-db",
            "type": "sqlite",
            "path": "/nonexistent/path/to/grafana.db",
            "batch_size": 1000,
        }
        result = backup_database(db_cfg, tmp_path, compress=False, filename_format="%d-%m-%Y")
        assert result["status"] == "connection_failed"
        assert "error" in result


# ---------------------------------------------------------------------------
# _interpolate_env / _interpolate_config
# ---------------------------------------------------------------------------

class TestInterpolateEnv:
    def test_replaces_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_PASS", "secret123")
        assert _interpolate_env("pass=${MY_PASS}") == "pass=secret123"

    def test_raises_on_missing_var(self, monkeypatch):
        monkeypatch.delenv("UNDEFINED_VAR", raising=False)
        with pytest.raises(ValueError, match="UNDEFINED_VAR"):
            _interpolate_env("${UNDEFINED_VAR}")

    def test_non_string_passthrough(self):
        assert _interpolate_env(42) == 42
        assert _interpolate_env(None) is None
        assert _interpolate_env(True) is True

    def test_interpolates_nested_dict(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "localhost")
        cfg = {"database": {"host": "${DB_HOST}", "port": 3306}}
        result = _interpolate_config(cfg)
        assert result["database"]["host"] == "localhost"
        assert result["database"]["port"] == 3306

    def test_interpolates_list(self, monkeypatch):
        monkeypatch.setenv("ITEM", "hello")
        result = _interpolate_config(["${ITEM}", "world"])
        assert result == ["hello", "world"]


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

class TestLoadConfig:
    def test_missing_file_returns_empty_dict(self, tmp_path):
        result = load_config(str(tmp_path / "nonexistent.yaml"))
        assert result == {}

    def test_loads_yaml(self, tmp_path):
        cfg_file = tmp_path / "cfg.yaml"
        cfg_file.write_text("backup:\n  compress: false\n")
        result = load_config(str(cfg_file))
        assert result["backup"]["compress"] is False

    def test_env_var_interpolated_in_yaml(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SECRET", "topsecret")
        cfg_file = tmp_path / "cfg.yaml"
        cfg_file.write_text("databases:\n  - password: ${SECRET}\n")
        result = load_config(str(cfg_file))
        assert result["databases"][0]["password"] == "topsecret"

    def test_empty_yaml_returns_empty_dict(self, tmp_path):
        cfg_file = tmp_path / "empty.yaml"
        cfg_file.write_text("")
        result = load_config(str(cfg_file))
        assert result == {}


# ---------------------------------------------------------------------------
# build_db_configs
# ---------------------------------------------------------------------------

class TestBuildDbConfigs:
    def test_sqlite_env_fallback(self, monkeypatch, tmp_path):
        db_path = tmp_path / "g.db"
        db_path.touch()
        monkeypatch.setenv("SQLITE_PATH", str(db_path))
        monkeypatch.delenv("MYSQL_HOST", raising=False)
        configs = build_db_configs({}, _make_args())
        assert len(configs) == 1
        assert configs[0]["type"] == "sqlite"
        assert configs[0]["path"] == str(db_path)

    def test_sqlite_env_custom_name(self, monkeypatch, tmp_path):
        db_path = tmp_path / "g.db"
        db_path.touch()
        monkeypatch.setenv("SQLITE_PATH", str(db_path))
        monkeypatch.setenv("SQLITE_NAME", "my-grafana")
        monkeypatch.delenv("MYSQL_HOST", raising=False)
        configs = build_db_configs({}, _make_args())
        assert configs[0]["name"] == "my-grafana"

    def test_db_filter_selects_matching_entry(self, monkeypatch):
        file_cfg = {
            "databases": [
                {"name": "db-a", "type": "sqlite", "path": "/a"},
                {"name": "db-b", "type": "sqlite", "path": "/b"},
            ]
        }
        configs = build_db_configs(file_cfg, _make_args(db=["db-a"]))
        assert len(configs) == 1
        assert configs[0]["name"] == "db-a"

    def test_db_filter_no_match_exits(self, monkeypatch):
        file_cfg = {"databases": [{"name": "db-a", "type": "sqlite", "path": "/a"}]}
        with pytest.raises(SystemExit):
            build_db_configs(file_cfg, _make_args(db=["nonexistent"]))

    def test_batch_size_default_applied(self):
        file_cfg = {
            "backup": {"batch_size": 500},
            "databases": [{"name": "x", "type": "sqlite", "path": "/x"}],
        }
        configs = build_db_configs(file_cfg, _make_args())
        assert configs[0]["batch_size"] == 500

    def test_per_db_batch_size_overrides_default(self):
        file_cfg = {
            "backup": {"batch_size": 500},
            "databases": [{"name": "x", "type": "sqlite", "path": "/x", "batch_size": 100}],
        }
        configs = build_db_configs(file_cfg, _make_args())
        assert configs[0]["batch_size"] == 100

    def test_no_config_no_env_exits(self, monkeypatch):
        monkeypatch.delenv("SQLITE_PATH", raising=False)
        monkeypatch.delenv("MYSQL_HOST", raising=False)
        with pytest.raises(SystemExit):
            build_db_configs({}, _make_args())


# ---------------------------------------------------------------------------
# make_connector
# ---------------------------------------------------------------------------

class TestMakeConnector:
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown database type"):
            make_connector({"type": "oracle"})

    def test_sqlite_returns_sqlite_connector(self, tmp_path):
        db_path = tmp_path / "test.db"
        sqlite3.connect(str(db_path)).close()
        conn = make_connector({"type": "sqlite", "path": str(db_path)})
        assert isinstance(conn, SQLiteConnector)
        conn.close()


# ---------------------------------------------------------------------------
# SQLiteConnector
# ---------------------------------------------------------------------------

class TestSQLiteConnector:
    @pytest.fixture()
    def db_with_data(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE users (id INTEGER, login TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'admin')")
        conn.execute("INSERT INTO users VALUES (2, 'editor')")
        conn.commit()
        conn.close()
        return db_path

    def test_get_tables(self, db_with_data):
        c = SQLiteConnector({"path": str(db_with_data)})
        assert "users" in c.get_tables()
        c.close()

    def test_get_row_count(self, db_with_data):
        c = SQLiteConnector({"path": str(db_with_data)})
        assert c.get_row_count("users") == 2
        c.close()

    def test_fetch_batch_returns_dicts(self, db_with_data):
        c = SQLiteConnector({"path": str(db_with_data)})
        rows = c.fetch_batch("users", 10, 0)
        assert isinstance(rows[0], dict)
        assert rows[0]["login"] == "admin"
        c.close()

    def test_fetch_batch_respects_limit_offset(self, db_with_data):
        c = SQLiteConnector({"path": str(db_with_data)})
        rows = c.fetch_batch("users", 1, 1)
        assert len(rows) == 1
        assert rows[0]["login"] == "editor"
        c.close()

    def test_missing_db_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            SQLiteConnector({"path": "/no/such/file.db"})

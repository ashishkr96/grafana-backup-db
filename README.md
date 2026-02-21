# MySQL Database Backup Tool

Schema-agnostic MySQL backup: auto-discovers every table, exports every row as JSON, and compresses into a timestamped archive. Works for Grafana or any MySQL database.

## Features

- **Schema-agnostic** — reads `SHOW TABLES` at runtime, survives schema changes
- **Multi-database** — back up multiple DBs in one run
- **On-call ready** — configure via file, env vars, or CLI flags
- **Large-table safe** — batch row fetching (no OOM)
- **Manifest file** — `manifest.json` in every archive with row counts, timestamps, status
- **Flexible output** — compressed `.tar.gz` (default) or raw directories

## Quick Start

```bash
pip install -r requirements.txt

# Option A: env vars only (no config file needed)
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=grafana
export MYSQL_PASSWORD=secret
export MYSQL_DATABASE=grafana
python backup.py

# Option B: config file
cp .env.example .env          # fill in secrets
export $(grep -v '^#' .env | xargs)
python backup.py              # uses config.yaml by default
```

## On-Call Runbook

Someone deleted a DB and you need to restore from the latest backup:

```bash
# 1. Get the DB credentials from your secrets manager / vault
export MYSQL_HOST=<host>
export MYSQL_USER=<user>
export MYSQL_PASSWORD=<pass>
export MYSQL_DATABASE=<dbname>

# 2. Run backup immediately (no config file needed)
python backup.py --output /tmp/incident-backup

# 3. Inspect the manifest
tar -xzf /tmp/incident-backup/*.tar.gz -C /tmp/restore
cat /tmp/restore/*/manifest.json | python -m json.tool
```

## CLI Reference

```
python backup.py [OPTIONS]

Options:
  --config PATH         Config YAML file (default: ./config.yaml)
  --db NAME [NAME ...]  Back up only these database label(s)
  --output DIR          Override output directory
  --no-compress         Keep raw directories instead of .tar.gz
  --batch-size N        Rows per DB fetch (default: 1000)
```

## Config File (`config.yaml`)

```yaml
backup:
  output_dir: ./backups
  compress: true
  batch_size: 1000

databases:
  - name: grafana-local
    host: 127.0.0.1
    port: 3306
    user: grafana
    password: ${GRAFANA_DB_PASS}   # env var interpolation
    database: grafana

  - name: grafana-prod
    host: ${GRAFANA_PROD_HOST}
    port: 3306
    user: ${GRAFANA_PROD_USER}
    password: ${GRAFANA_PROD_PASS}
    database: grafana
```

## Config Priority

```
CLI args  >  Environment variables  >  config.yaml  >  defaults
```

## Backup Structure

```
backups/
└── grafana-local_2026-02-21_14-30-00.tar.gz
    └── grafana-local_2026-02-21_14-30-00/
        ├── manifest.json          ← row counts, timestamps, table list
        ├── dashboard/
        │   ├── row_0.json
        │   ├── row_1.json
        │   └── ...
        ├── alert/
        │   └── ...
        └── user/
            └── ...
```

### manifest.json

```json
{
  "database_label": "grafana-local",
  "host": "127.0.0.1",
  "database": "grafana",
  "started_at": "2026-02-21T14:30:00",
  "completed_at": "2026-02-21T14:30:12",
  "status": "success",
  "total_tables": 28,
  "total_rows": 4823,
  "tables": {
    "dashboard": { "rows": 142 },
    "alert":     { "rows": 37 },
    "user":      { "rows": 5 }
  }
}
```

## Scheduling

```bash
# Cron: daily at 2am
0 2 * * * cd /opt/grafana-backup && export $(cat .env | xargs) && python backup.py >> /var/log/db-backup.log 2>&1
```

## Multiple DBs

```bash
# Backup all databases defined in config.yaml
python backup.py

# Backup only specific ones
python backup.py --db grafana-local grafana-prod
```

## Maintainers

| Name | GitHub |
|------|--------|
| Ashish Choubey | [@ashishkr96](https://github.com/ashishkr96) |

## Contributing

Contributions are welcome. Please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

Copyright (c) 2026 Ashish Choubey

### Enterprise Use

This software is free to use in commercial and enterprise environments under the MIT License. If you use this tool within your organization or as part of a commercial product or service, attribution is appreciated:

> Powered by [grafana-backup-db](https://github.com/ashish/grafana-backup-db) by Ashish Choubey

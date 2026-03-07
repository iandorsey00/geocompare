# GeoCompare

GeoCompare is a program that allows users to easily view and organized and
processed demographic data without the need to modify any data files.

See the [Wiki](https://github.com/iandorsey00/geodata/wiki) for documentation.
Architecture details: [doc/architecture.md](doc/architecture.md).
Versioning policy: [doc/versioning.md](doc/versioning.md).

## Install

```bash
python3 -m pip install -e .
```

## CLI Quick Start

Build data products:

```bash
geocompare build /path/to/data
```

Query workflows:

```bash
geocompare query search "san francisco"
geocompare query profile "San Francisco city, California"
geocompare query top population -n 10
geocompare query nearest "San Francisco city, California" -n 10
geocompare query distance "San Francisco city, California" "San Jose city, California"
geocompare resolve "San Francisco, CA" --state ca -n 5
```

Export workflows:

```bash
geocompare export rows "population :income" -n 20
geocompare export profile "San Francisco city, California"
```

## Diagnostics

Set verbosity when needed:

```bash
geocompare --log-level INFO build /path/to/data
```

## Quality Baseline

Run local quality checks:

```bash
python3 -m pip install -e ".[dev]"
ruff check tests geodata/identity geodata/repository/sqlite_repository.py geodata/interfaces/cli.py scripts/benchmark_queries.py
black --check tests geodata/identity geodata/repository/sqlite_repository.py geodata/interfaces/cli.py scripts/benchmark_queries.py
mypy geodata/identity geodata/repository/sqlite_repository.py geodata/interfaces/cli.py
pytest -q
```

## Storage Model

- SQLite is the only data backend (`bin/default.sqlite`).
- Repository metadata now includes a `schema_version` table.
- Schema upgrades use explicit, step-based migrations in `SQLiteRepository`.

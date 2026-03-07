# geodata

geodata is a program that allows users to easily view and organized and
processed demographic data without the need to modify any data files.

![geodata screenshot](https://raw.githubusercontent.com/iandorsey00/geodata/master/doc/img/geodata-screenshot.png "geodata screenshot")

This project supports geographies in the United States.

See the [Wiki](https://github.com/iandorsey00/geodata/wiki) for documentation.
Architecture details: [doc/architecture.md](doc/architecture.md).

## Install

```bash
python3 -m pip install -e .
```

## CLI Quick Start

Build data products:

```bash
geodata build /path/to/data
```

Query workflows:

```bash
geodata query search "san francisco"
geodata query profile "San Francisco city, California"
geodata query top population -n 10
geodata query nearest "San Francisco city, California" -n 10
geodata query distance "San Francisco city, California" "San Jose city, California"
geodata resolve "San Francisco, CA" --state ca -n 5
```

Export workflows:

```bash
geodata export rows "population :income" -n 20
geodata export profile "San Francisco city, California"
```

## Diagnostics

Set verbosity when needed:

```bash
geodata --log-level INFO build /path/to/data
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

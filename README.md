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

Fetch latest ACS + gazetteer files (with resumable downloads and progress bars):

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data
```

Refresh an existing data directory safely (recommended):

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --archive-existing
```

Delete only managed ACS/gazetteer files before refetching:

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --clean
```

Rate-limit-sensitive run (fails fast on Cloudflare 1015 / HTTP 429):

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --archive-existing --max-attempts 2
```

`build` now auto-detects the latest ACS year (`g<YEAR>5*.csv` + `e<YEAR>5*.txt`)
and latest compatible gazetteer year (`<YEAR>_Gaz_*_national.txt`) in the input
directory.

Optional overlays can be placed in the same data directory:

- `overlays/crime_data.csv` (or `crime_data.csv`)
- `overlays/project_data.csv` (or `project_data.csv`)
- `overlays/social_alignment.csv`

Overlay files should include a `GEOID` column plus numeric metric columns.
Crime metrics (column names containing `crime`) appear under a `CRIME` section
in demographic profiles. Other overlay metrics appear under `PROJECT DATA`.

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

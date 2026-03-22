# GeoCompare

GeoCompare is a CLI for building and querying standardized local demographic
data products from public datasets, without manual data-file editing.

Documentation: [doc/index.md](doc/index.md).
Architecture details: [doc/architecture.md](doc/architecture.md).
Versioning policy: [doc/versioning.md](doc/versioning.md).

## Install

```bash
python3 -m pip install -e .
```

## CLI Quick Start

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data
python3 -m geocompare.interfaces.cli build /path/to/data
geocompare query search "san francisco"
geocompare query profile "San Francisco city, California"
```

For setup, commands, overlays, and deeper reference material, use the docs in
[`doc/`](doc/index.md).

## Optional API

GeoCompare also includes a minimal read-only API layer for personal remote
access and lightweight deployments.

```bash
python3 -m pip install -e ".[web]"
geocompare-api
```

See [`doc/remote-access.md`](doc/remote-access.md) for details.

## Diagnostics

Set verbosity when needed:

```bash
geocompare --log-level INFO build /path/to/data
```

## Quality Baseline

Run local quality checks:

```bash
python3 -m pip install -e ".[dev]"
ruff check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py
black --check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py
mypy geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py
pytest -q
```

# GeoCompare

GeoCompare is a CLI for building and querying standardized local demographic
data products from public datasets, without manual data-file editing.

See the [Wiki](https://github.com/iandorsey00/geocompare/wiki) for documentation.
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

GeoCompare supports two overlay types:

- Built-in overlays: canonical crime and voter files.
- Custom overlays: private/user-defined metrics (including submodule-fed data).

Optional overlays can be placed in the same data directory:

- `overlays/crime_data.csv`
- `overlays/voter_data.csv`
- `overlays/project_data.csv`

Overlay files should include a `GEOID` column plus numeric metric columns.
You can also normalize sources into canonical overlays automatically:

```bash
python3 scripts/fetch_overlays.py \
  --out-dir /path/to/data \
  --crime-source /path/or/url/to/crime.csv \
  --voter-source /path/or/url/to/voter.csv
```

Crime metrics appear under `CRIME`, voter metrics under `VOTER REGISTRATION`,
and custom metrics in `project_data.csv` appear under `PROJECT DATA`.

### Custom Overlay Conventions

For custom overlays (for example, private submodule outputs):

- Keep one canonical CSV with `GEOID` and numeric metric columns.
- Prefer `project_` prefixes for private/custom identifiers.
- Use `_pct` suffix for percentages.

Example `project_data.csv`:

```csv
GEOID,project_custom_score,project_custom_confidence_pct
06037,63.2,91.5
06073,58.8,88.1
```

Recommended metadata file (`overlay_manifest.json`) in your overlay repo:

```json
{
  "overlay": "custom-overlay",
  "metrics": [
    {
      "key": "project_custom_score",
      "label": "Custom score",
      "section": "PROJECT DATA",
      "type": "score"
    },
    {
      "key": "project_custom_confidence_pct",
      "label": "Custom confidence",
      "section": "PROJECT DATA",
      "type": "pct"
    }
  ]
}
```

`overlay_manifest.json` is optional today, but recommended for stable naming,
labels, and section placement across overlay builds.

## Base-Only Rebuild (No Custom Overlay)

Use this path to return to a clean, shareable base geocompare state:

1. Fetch ACS + Gazetteer files:

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --archive-existing
```

2. (Optional) refresh canonical built-in overlays:

```bash
python3 scripts/fetch_overlays.py \
  --out-dir /path/to/data \
  --crime-source /path/or/url/to/crime.csv \
  --voter-source /path/or/url/to/voter.csv
```

3. Ensure no private overlay file is present:

- Remove or relocate `/path/to/data/overlays/project_data.csv`
- Remove or relocate `/path/to/data/overlays/overlay_manifest.json`

4. Build:

```bash
python3 -m geocompare.interfaces.cli build /path/to/data
```

## Repository Scripts

Tracked operational scripts:

- `scripts/fetch_latest_acs.py`: download/update ACS + Gazetteer inputs.
- `scripts/fetch_overlays.py`: normalize built-in crime/voter overlays.
- `scripts/build_nibrs_crime_overlay.py`: build base crime overlay from NIBRS inputs.

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
ruff check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py
black --check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py
mypy geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py
pytest -q
```

## Storage Model

- SQLite is the only data backend (`bin/default.sqlite`).
- Repository metadata now includes a `schema_version` table.
- Schema upgrades use explicit, step-based migrations in `SQLiteRepository`.

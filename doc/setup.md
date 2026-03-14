# Setup

If setup fails at any step, capture the command and full error text.

## Supported platforms

- macOS is the primary supported platform.
- Linux and WSL should work with minor or no changes.
- Windows may work, but current development/testing is focused on Unix-like shells.

## Prerequisites

- Python `3.9+`
- `unzip`
- Enough free disk space for ACS and gazetteer inputs

## Install GeoCompare

```bash
git clone https://github.com/iandorsey00/geocompare.git
cd geocompare
python3 -m pip install -e .
```

## Prepare source data

The recommended workflow is to fetch the latest compatible ACS and gazetteer
files automatically:

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data
```

Useful variants:

```bash
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --archive-existing
python3 scripts/fetch_latest_acs.py --out-dir /path/to/data --clean
```

Manual download still works. Put ACS files, geography files, the ACS table
lookup file, and gazetteer extracts in one directory.

## Optional overlays

Supported overlay files live under:

```text
<data_dir>/overlays/
```

Canonical filenames:

- `crime_data.csv`
- `voter_data.csv`
- `project_data.csv`

You can build canonical crime and voter overlays from raw sources:

```bash
python3 scripts/fetch_overlays.py \
  --out-dir /path/to/data \
  --crime-source /path/or/url/to/crime.csv \
  --voter-source /path/or/url/to/voter.csv
```

`scripts/fetch_overlays.py` merges onto existing canonical overlay files, which
is useful for incremental state-by-state voter imports.

For overlay naming and manifest guidance, see [Overlays](./overlays.md).

## Build data products

```bash
geocompare build /path/to/data
```

GeoCompare auto-detects the latest ACS year and latest compatible gazetteer
year present in the input directory.

## Verify installation

```bash
geocompare query search "San Francisco" -n 5
geocompare query profile "San Francisco city, California"
```

## Troubleshooting

- `command not found: geocompare`
  - Re-run `python3 -m pip install -e .` from the repo root.
- Build fails with missing files
  - Confirm ACS files, geography files, lookup file, and gazetteer extracts are present.
- `Sorry, no geographies match your criteria.`
  - Broaden `--scope` or `--where` filters and verify the data identifier name.
- Exact geography name not found
  - Use `geocompare query search "<name>"` first, then copy the exact display label.

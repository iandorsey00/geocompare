# GeoCompare Handoff

## Snapshot
- Project: `geocompare`
- Handoff date: 2026-03-08
- Branch: `master`
- Version: `0.6.12`

## Project Scope
GeoCompare builds and queries local demographic data products from ACS/Gazetteer
inputs and optional overlays.

Project documentation now lives in-repo under `doc/`. The standalone
`geocompare.wiki` repo can be treated as deprecated once the migrated docs are
committed and pushed.

Core/base overlays:
- Crime overlay (`CRIME`)
- Voter registration overlay (`VOTER REGISTRATION`)

Optional/custom overlays:
- User/private metrics, typically in `project_data.csv` (`PROJECT DATA` or
  manifest-defined section)

## Current Data/Overlay Model
- Build command:
  - `python3 -m geocompare.interfaces.cli build <data_path>`
- Base inputs are discovered from files under `<data_path>`.
- Overlay inputs are discovered under `<data_path>/overlays`.
- `scripts/fetch_overlays.py` merges onto existing canonical overlay CSVs, so
  voter/crime data can be imported incrementally across multiple runs.
- Built-in voter overlays may be partial:
  - `registered_voters` alone is valid.
  - Party breakout columns are optional when a source does not publish them.
- Optional manifest support:
  - `overlay_manifest.json` (or `manifest.json`) in overlays directory.
  - Supports per-metric metadata (`key`, `label`, `section`, `type`, `order`).
- Overlay section placement:
  - Base profile sections first.
  - Overlay sections appended at bottom.
  - Overlay rows deterministically ordered.

## Base-Only Recovery (No Custom Overlay)
To restore a clean base state without private project overlay:

1. Fetch core data:
   - `python3 scripts/fetch_latest_acs.py --out-dir <data_path> --archive-existing`
2. Optionally build canonical base overlays:
   - `python3 scripts/fetch_overlays.py --out-dir <data_path> --crime-source <src> --voter-source <src>`
3. Ensure custom overlay artifacts are absent:
   - remove/relocate `<data_path>/overlays/project_data.csv`
   - remove/relocate `<data_path>/overlays/overlay_manifest.json`
4. Rebuild:
   - `python3 -m geocompare.interfaces.cli build <data_path>`

## Tracked Scripts
- `scripts/fetch_latest_acs.py`
- `scripts/fetch_overlays.py`
- `scripts/build_nibrs_crime_overlay.py`

## Validation
Recommended checks before ACP:

1. `ruff check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py`
2. `black --check tests geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py scripts/fetch_overlays.py`
3. `mypy geocompare/identity geocompare/repository/sqlite_repository.py geocompare/interfaces/cli.py`
4. `PYTHONPATH=. pytest -q`

## License
Repository license is MIT (`LICENSE`). This remains appropriate for the base
project.

# GeoCompare Handoff

## Snapshot
- Project: `geocompare`
- Handoff date: 2026-03-08
- Branch: `master`
- Version: `0.6.2`
- Last completed commit at handoff prep: `9d11bd7` (`Bump version to 0.6.2`)

## Purpose
GeoCompare builds and queries local demographic/geographic data products (Gazetteer + ACS-derived inputs) and supports ranking, distance, nearest geographies, and profile exports.

## Environment
- Python: 3.9+
- Package/deps: managed via `pyproject.toml`
- Common setup:
  - `python3 -m pip install -e ".[dev]"`

## Canonical CLI (Current)
Use canonical commands and flags only.

- Build data products:
  - `python3 -m geocompare.interfaces.cli build <data_path>`
- Search:
  - `python3 -m geocompare.interfaces.cli query search "san francisco"`
- Profile:
  - `python3 -m geocompare.interfaces.cli query profile "San Francisco city, California"`
- Top/bottom with modern filter and scope:
  - `python3 -m geocompare.interfaces.cli query top median_year_structure_built --where 'population>=100000' --universe places --in-state ca`
  - `python3 -m geocompare.interfaces.cli query bottom median_household_income --where 'population>=50000' --scope places+ca`
- Nearest:
  - `python3 -m geocompare.interfaces.cli query nearest "San Francisco city, California" --where 'population>=100000' --universe places --in-state ca -n 10`
- Resolve:
  - `python3 -m geocompare.interfaces.cli resolve "San Francisco, CA" --state ca -n 5`
- Export rows:
  - `python3 -m geocompare.interfaces.cli export rows ":population :income" --where 'population>=100000' --universe places --in-state ca`

## Important Interface Decisions
- Legacy CLI aliases were purged.
  - Removed command aliases like `hv`, `lv`, `cg`, `dist`, `dp`, etc.
  - Removed legacy flag names `--geofilter` and `--context`.
- Canonical query flags are:
  - Filter: `--where` (short: `-w`)
  - Scope string: `--scope` (short: `-s`)
  - Explicit scope composition: `--universe`, plus one of `--in-state|--in-county|--in-zcta`
- Legacy tool shims were removed.
  - Deleted: `CountyTools.py`, `StateTools.py`, `KeyTools.py`, `SummaryLevelTools.py`
  - Canonical modules: `county_lookup.py`, `state_lookup.py`, `county_key_index.py`, `summary_level_parser.py`

## Shell Caveat
- In `zsh`, quote or escape filter expressions that contain `>` or `<`.
  - Good: `--where 'population>=100000'`
  - Good: `--where population\>=100000`

## Data Expectations
- Build command expects source files under the provided `<data_path>`.
- The project has support for recent ACS table-based inputs and latest Gazetteer-era ingestion logic integrated in prior updates.
- If refreshing download logic, verify source year discovery against the files present in `<data_path>`.

## Validation / Definition of Done
Before ACP:
1. `ruff check geocompare/interfaces/cli.py geocompare/engine.py geocompare/tools tests`
2. `python3 -m pytest -q`
3. Run at least one smoke query:
   - `python3 -m geocompare.interfaces.cli --version`
   - `python3 -m geocompare.interfaces.cli query search "san francisco" -n 3`
   - `python3 -m geocompare.interfaces.cli query top median_year_structure_built --where 'population>=100000' --universe places --in-state ca -n 5`

## ACP Workflow
- Stage only intended files.
- Commit message should describe functional change precisely.
- Push to `origin/master` unless directed otherwise.

## Immediate Backlog Suggestions
1. Add CLI integration tests that invoke argument parsing paths for `--where`, `--scope`, and explicit scope flags.
2. Document canonical CLI examples in `README.md` and remove any remaining historical examples.
3. Optionally add `--where` parser help examples for compound (`:c`/`:cc`) usage.

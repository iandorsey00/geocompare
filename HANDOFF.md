# GeoCompare Handoff

## Snapshot

- Project: `geocompare`
- Branch: `master`
- Version: `0.12.0`

## Scope

GeoCompare builds and queries local demographic data products from:

- ACS 5-year summary-file inputs
- Census geography metadata
- built-in crime and voter overlays
- optional custom overlays

The project supports both CLI usage and a minimal read-only API for personal
remote access or lightweight deployments.

## Major Current Capabilities

- tract support throughout build, query, and identity resolution
- human-friendly tract labels, with optional official Census tract labels
- ranking queries such as:
  - `top`
  - `bottom`
  - `nearest`
  - `remoteness`
  - `local-average`
- map-link generation for CLI-selected geographies, including road-biased Street View targeting when boundary data is available
- human-friendly numeric CLI arguments, including comma-separated values like `1,000,000`
- GeoVector similarity in two modes:
  - `similar` for broad demographic similarity
  - `similar-form` for built-form and housing similarity
- candidate-side and qualifying-side remoteness filtering
- county-proxy filtering for large-county exploration
- built-in `sources` command for ACS and base-overlay provenance
- optional API via `geocompare-api`

## Data Model Notes

- Base profile metrics primarily come from ACS 5-year estimates.
- Geography metadata such as land area and coordinates comes from Census
  Gazetteer files.
- Built-in overlays:
  - `CRIME`
  - `VOTER REGISTRATION`
- Custom overlays may also be attached during build.
- `sources` lists built-in sources only; it intentionally excludes personal or
  custom overlay metrics.

## Main Entry Points

- Build:
  - `python3 -m geocompare.interfaces.cli build <data_path>`
- Common queries:
  - `geocompare query search ...`
  - `geocompare query profile ...`
  - `geocompare query map-links ...`
  - `geocompare query similar ...`
  - `geocompare query similar-form ...`
  - `geocompare query remoteness ...`
  - `geocompare query local-average ...`
  - `geocompare sources`

## Documentation

Primary docs live under [`doc/`](./doc/index.md), especially:

- [Setup](./doc/setup.md)
- [Commands](./doc/commands.md)
- [Argument Types](./doc/argument-types.md)
- [Overlays](./doc/overlays.md)
- [Remote Access](./doc/remote-access.md)

## Validation

Useful checks before ACP:

1. `ruff check tests geocompare`
2. `black --check tests geocompare`
3. `PYTHONPATH=. pytest -q`

Use narrower commands when working on a small slice of the repo.

## Notes

- Keep repo docs project-neutral and avoid embedding private deployment details.
- Prefer separate sibling repos for richer web UI or geocoding products rather
  than expanding the core repo too far beyond its build/query role.

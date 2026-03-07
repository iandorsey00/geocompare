# Versioning Policy

GeoCompare uses Semantic Versioning (`MAJOR.MINOR.PATCH`).

- `MAJOR`: breaking CLI/API behavior changes
- `MINOR`: backward-compatible features and UX improvements
- `PATCH`: bug fixes and internal maintenance

Current version is defined in `geocompare/__init__.py` as `__version__`.
Packaging metadata reads this value via `pyproject.toml` so version stays single-source.

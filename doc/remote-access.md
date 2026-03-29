# Remote Access

GeoCompare can expose a minimal read-only web API for personal remote access.

This API is intended as a lightweight wrapper around the existing query engine,
not as a full hosted application.

## Install

Install the optional web dependencies:

```bash
python3 -m pip install -e ".[web]"
```

## Run locally

```bash
geocompare-api
```

Environment variables:

- `GEOCOMPARE_SQLITE_PATH`
  - path to the SQLite data product
- `GEOCOMPARE_API_HOST`
  - default: `127.0.0.1`
- `GEOCOMPARE_API_PORT`
  - default: `8000`

Example:

```bash
GEOCOMPARE_SQLITE_PATH=/path/to/default.sqlite \
GEOCOMPARE_API_HOST=0.0.0.0 \
GEOCOMPARE_API_PORT=8000 \
geocompare-api
```

## Endpoints

- `GET /health`
- `GET /search?q=...&n=...`
- `GET /profile?name=...`
- `GET /similar?name=...`
- `GET /similar-form?name=...`
- `GET /resolve?query=...`
- `GET /remoteness?...`
- `GET /local-average?...`

These endpoints are read-only and return JSON.

Similarity API notes:

- `similar` and `similar-form` support:
  - `universe`
  - `universes`
  - `in_state`
  - `in_county`
  - `in_zcta`
- `universe` and `universes` are mutually exclusive
- use at most one of:
  - `in_state`
  - `in_county`
  - `in_zcta`

## Deployment notes

- A `systemd` example unit is provided at `deploy/geocompare-api.service`.
- Prefer binding the API to `127.0.0.1` and placing a reverse proxy such as
  Caddy or Nginx in front of it.
- Keep the first deployment minimal:
  - one app process
  - one SQLite file
  - no background workers
  - no writable public endpoints

## Scope

The API is appropriate for personal remote access and as a stepping stone
toward a richer web interface.

For a full map-first product with tract boundary rendering and client-side
interactivity, a separate frontend project is usually the cleaner next step.

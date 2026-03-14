# Technical Documentation

This page points to the current architecture docs and code entry points.

## Start here

- [Architecture](./architecture.md)
- [Versioning](./versioning.md)

## Important code locations

- `geocompare/interfaces`
- `geocompare/services`
- `geocompare/engine.py`
- `geocompare/repository`
- `geocompare/database`

## Current architecture summary

- CLI and GUI are the user-facing interfaces.
- The engine coordinates query workflows and build orchestration.
- SQLite is the runtime storage backend.
- The build pipeline ingests Census inputs plus optional overlays.

Older historical docs referred to pickle-based storage and legacy module
layouts. The current system is SQLite-based.

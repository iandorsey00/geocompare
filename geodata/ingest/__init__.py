try:
    from geodata.ingest.database_builder import DatabaseBuilder
except ImportError:  # pragma: no cover - script execution fallback
    from ingest.database_builder import DatabaseBuilder

__all__ = ['DatabaseBuilder']

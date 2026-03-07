try:
    from geodata.database.Database import Database as DatabaseBuilder
except ImportError:  # pragma: no cover - script execution fallback
    from database.Database import Database as DatabaseBuilder

__all__ = ['DatabaseBuilder']

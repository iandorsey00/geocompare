try:
    from geodata.repository.base import DataRepository
    from geodata.repository.sqlite_repository import SQLiteRepository
except ImportError:  # pragma: no cover - script execution fallback
    from repository.base import DataRepository
    from repository.sqlite_repository import SQLiteRepository

__all__ = ['DataRepository', 'SQLiteRepository']

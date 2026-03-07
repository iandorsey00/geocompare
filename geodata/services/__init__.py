try:
    from geocompare.services.query_service import QueryService
except ImportError:  # pragma: no cover - script execution fallback
    from geodata.services.query_service import QueryService

__all__ = ['QueryService']

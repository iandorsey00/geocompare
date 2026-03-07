try:
    from geodata.datainterface.GeoVector import GeoVector
except ImportError:  # pragma: no cover - script execution fallback
    from datainterface.GeoVector import GeoVector

__all__ = ['GeoVector']

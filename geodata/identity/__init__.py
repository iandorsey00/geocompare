try:
    from geodata.identity.place_identity import PlaceIdentityIndex
except ImportError:  # pragma: no cover - script execution fallback
    from identity.place_identity import PlaceIdentityIndex

__all__ = ['PlaceIdentityIndex']

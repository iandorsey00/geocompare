try:
    from geocompare.models.demographic_profile import DemographicProfile
    from geocompare.models.geovector import GeoVector
except ImportError:  # pragma: no cover - script execution fallback
    try:
        from geodata.models.demographic_profile import DemographicProfile
        from geodata.models.geovector import GeoVector
    except ImportError:  # pragma: no cover - script execution fallback
        from models.demographic_profile import DemographicProfile
        from models.geovector import GeoVector

__all__ = ['DemographicProfile', 'GeoVector']

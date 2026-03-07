try:
    from geodata.datainterface.DemographicProfile import DemographicProfile
except ImportError:  # pragma: no cover - script execution fallback
    from datainterface.DemographicProfile import DemographicProfile

__all__ = ['DemographicProfile']

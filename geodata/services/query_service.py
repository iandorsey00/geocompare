try:
    from geodata.engine import Engine
except ImportError:  # pragma: no cover - script execution fallback
    from engine import Engine


class QueryService(Engine):
    '''Backward-compatible service facade over Engine.'''


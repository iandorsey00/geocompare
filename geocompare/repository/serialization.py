import json
from types import SimpleNamespace

import numpy as np

from geocompare.models.demographic_profile import DemographicProfile
from geocompare.models.geovector import GeoVector

_KNOWN_TYPES = {
    "geocompare.models.demographic_profile.DemographicProfile": DemographicProfile,
    "geocompare.models.geovector.GeoVector": GeoVector,
}


def _type_name(value):
    cls = value.__class__
    return f"{cls.__module__}.{cls.__name__}"


def _to_jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if hasattr(value, "__dict__"):
        return {
            "__type__": _type_name(value),
            "attrs": _to_jsonable(value.__dict__),
        }
    raise TypeError(f"unsupported type for serialization: {type(value)!r}")


def _to_object(value):
    if isinstance(value, list):
        return [_to_object(item) for item in value]
    if not isinstance(value, dict):
        return value

    tagged_type = value.get("__type__")
    if not tagged_type:
        return {key: _to_object(item) for key, item in value.items()}

    attrs = _to_object(value.get("attrs", {}))
    cls = _KNOWN_TYPES.get(tagged_type)
    if cls is None:
        obj = SimpleNamespace()
        obj.__dict__.update(attrs)
        return obj

    obj = cls.__new__(cls)
    obj.__dict__.update(attrs)
    return obj


def dump_payload(value):
    return json.dumps(_to_jsonable(value), separators=(",", ":")).encode("utf-8")


def load_payload(payload):
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    return _to_object(json.loads(payload))

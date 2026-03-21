from geocompare.tools.county_lookup import CountyLookup
from geocompare.tools.state_lookup import StateLookup


_COUNTY_LOOKUP = CountyLookup()
_STATE_LOOKUP = StateLookup()
_PLACE_SUFFIXES = (
    " city",
    " town",
    " village",
    " borough",
    " municipality",
    " cdp",
    " census designated place",
)


def format_tract_code(tract_code):
    """Format a six-digit tract code using the common decimal style."""
    raw = str(tract_code or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) != 6:
        return raw

    whole = int(digits[:4])
    fractional = digits[4:]
    if fractional == "00":
        return str(whole)
    return f"{whole}.{fractional}"


def tract_display_name_from_geoid(geoid):
    """Build a more human-friendly tract label from a tract GEOID."""
    raw = str(geoid or "").strip()
    suffix = raw.split("US", 1)[1] if "US" in raw else raw
    digits = "".join(ch for ch in suffix if ch.isdigit())
    if len(digits) < 11:
        return raw

    state_geoid = digits[:2]
    county_geoid = digits[:5]
    tract_code = digits[5:11]

    county_name = _COUNTY_LOOKUP.county_geoid_to_name.get(county_geoid)
    state_name = _STATE_LOOKUP.geoid_to_name.get(state_geoid)
    tract_label = format_tract_code(tract_code)

    if county_name and state_name:
        county_stem = county_name.split(", ")[0]
        return f"Census Tract {tract_label}, {county_stem}, {state_name}"
    return f"Census Tract {tract_label}"


def compact_place_name(name):
    """Trim Census place suffixes and trailing state metadata for display."""
    text = str(name or "").strip()
    if ";" in text:
        text = text.split(";", 1)[0]
    elif "," in text:
        text = text.split(",", 1)[0]

    lowered = text.lower()
    for suffix in _PLACE_SUFFIXES:
        if lowered.endswith(suffix):
            text = text[: -len(suffix)].rstrip()
            break
    return text


def humanized_tract_name(geoid, nearby_place_name=None, state_abbrev=None):
    """Build a compact tract label aimed at human-readable table output."""
    raw = str(geoid or "").strip()
    suffix = raw.split("US", 1)[1] if "US" in raw else raw
    digits = "".join(ch for ch in suffix if ch.isdigit())
    if len(digits) < 11:
        return raw

    county_geoid = digits[:5]
    tract_code = digits[5:11]
    tract_label = format_tract_code(tract_code)
    county_name = _COUNTY_LOOKUP.county_geoid_to_name.get(county_geoid)
    county_stem = county_name.split(", ")[0] if county_name else ""
    state_code = state_abbrev.upper() if state_abbrev else digits[:2]

    parts = [tract_label]
    place_label = compact_place_name(nearby_place_name)
    if place_label:
        parts.append(f"near {place_label}")
    if county_stem:
        parts.append(county_stem)
    if state_code:
        parts.append(state_code)
    return ", ".join(parts)


def county_geoids_for_geography(geoid, sumlevel):
    """Resolve county GEOID containment for supported summary levels."""
    raw = str(geoid or "").strip()
    suffix = raw.split("US", 1)[1] if "US" in raw else raw
    digits = "".join(ch for ch in suffix if ch.isdigit())

    if sumlevel == "160":
        return list(_COUNTY_LOOKUP.place_to_counties.get(digits, []))
    if sumlevel == "140" and len(digits) >= 5:
        return [digits[:5]]
    return []


def county_display_names(county_geoids):
    """Render county names without the trailing state name."""
    names = []
    for county_geoid in county_geoids or []:
        county_name = _COUNTY_LOOKUP.county_geoid_to_name.get(county_geoid)
        if not county_name:
            continue
        names.append(county_name.split(", ")[0])
    return names

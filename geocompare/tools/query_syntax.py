import re
from typing import Dict, List, Optional

from geocompare.tools.county_lookup import CountyLookup
from geocompare.tools.state_lookup import StateLookup

_VALID_OPERATOR_KEYS = {"gt", "gteq", "eq", "lteq", "lt"}
_SYMBOL_TO_OPERATOR = {
    ">": "gt",
    ">=": "gteq",
    "=": "eq",
    "<=": "lteq",
    "<": "lt",
}

_SYMBOL_FILTER_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|=|>|<)\s*(.+?)\s*$")
_WORD_FILTER_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(gt|gteq|eq|lteq|lt)\s+(.+?)\s*$")
_COUNTY_GEOID_RE = re.compile(r"^\d{5}:county$")
_COUNTY_KEY_RE = re.compile(r"^[a-z]{2}:[a-z0-9]+$")
_COUNTY_US_KEY_RE = re.compile(r"^us:[a-z]{2}:[a-z0-9]+/county$")

_CT = CountyLookup()
_ST = StateLookup()
_COUNTY_NAME_TO_GEOID_LOWER = {
    county_name.lower(): geoid for county_name, geoid in _CT.county_name_to_geoid.items()
}
_STATE_NAME_TO_ABBREV_LOWER = {
    state_name.lower(): abbrev.lower() for state_name, abbrev in _ST.name_to_abbrev.items()
}


def parse_geofilter(geofilter: str) -> List[Dict[str, Optional[str]]]:
    """Parse filter criteria in legacy and modern CLI forms."""
    if not geofilter:
        return []

    criteria: List[Dict[str, Optional[str]]] = []
    raw_criteria = [
        token.strip() for token in geofilter.replace(",", "+").split("+") if token.strip()
    ]

    for raw in raw_criteria:
        parsed = _parse_single_filter(raw)
        criteria.append(parsed)

    return criteria


def _parse_single_filter(raw: str) -> Dict[str, Optional[str]]:
    symbol_match = _SYMBOL_FILTER_RE.match(raw)
    if symbol_match:
        comp, operator_symbol, value = symbol_match.groups()
        normalized_value = value.strip()
        if re.search(r":\s*(c|cc)\s*$", normalized_value):
            raise ValueError(
                "filter: Legacy :c/:cc suffixes are no longer supported. "
                "Use data identifiers directly (for example: bachelors_degree_or_higher_pct>=40)."
            )
        return {
            "comp": comp,
            "operator": _SYMBOL_TO_OPERATOR[operator_symbol],
            "value": normalized_value,
        }

    word_match = _WORD_FILTER_RE.match(raw)
    if word_match:
        comp, operator_key, value = word_match.groups()
        normalized_value = value.strip()
        if re.search(r":\s*(c|cc)\s*$", normalized_value):
            raise ValueError(
                "filter: Legacy :c/:cc suffixes are no longer supported. "
                "Use data identifiers directly (for example: bachelors_degree_or_higher_pct>=40)."
            )
        return {
            "comp": comp,
            "operator": operator_key,
            "value": normalized_value,
        }

    raise ValueError(
        "filter: Invalid criteria. Use 'data_identifier>=value' " "(operators: >,>=,=,<=,<)."
    )


def build_context(
    context: Optional[str] = None,
    universe: Optional[str] = None,
    in_state: Optional[str] = None,
    in_county: Optional[str] = None,
    in_zcta: Optional[str] = None,
) -> str:
    """Build legacy context string from explicit scope options."""
    has_explicit_scope = any([universe, in_state, in_county, in_zcta])
    if context and has_explicit_scope:
        raise ValueError("Use either --scope or explicit --universe/--in-* options.")
    if context:
        return context

    group = None
    if in_state:
        group = _normalize_in_state(in_state)
    elif in_county:
        group = _normalize_in_county(in_county)
    elif in_zcta:
        group = in_zcta

    normalized_universe = universe.lower() if universe else None

    if normalized_universe and group:
        return f"{normalized_universe}+{group}"
    if normalized_universe:
        return f"{normalized_universe}+"
    if group:
        return group
    return ""


def _normalize_in_state(in_state: str) -> str:
    raw = str(in_state or "").strip()
    if not raw:
        return raw

    upper = raw.upper()
    if upper in _ST.get_abbrevs(inc_us=True):
        return upper.lower()

    return _STATE_NAME_TO_ABBREV_LOWER.get(raw.lower(), raw.lower())


def _normalize_in_county(in_county: str) -> str:
    raw = str(in_county or "").strip()
    lower = raw.lower()

    if _COUNTY_GEOID_RE.match(lower):
        return lower
    if _COUNTY_KEY_RE.match(lower):
        return lower
    if _COUNTY_US_KEY_RE.match(lower):
        return lower[3:-7]

    geoid = _CT.county_name_to_geoid.get(raw)
    if geoid:
        return f"{geoid}:county"

    geoid = _COUNTY_NAME_TO_GEOID_LOWER.get(lower)
    if geoid:
        return f"{geoid}:county"

    raise ValueError(
        "Invalid --in-county value. Use 06037:county, ca:losangeles, "
        "or a full county name like 'Los Angeles County, California'."
    )

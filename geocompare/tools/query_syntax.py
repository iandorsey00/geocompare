import re
from typing import Dict, List, Optional

_VALID_OPERATOR_KEYS = {"gt", "gteq", "eq", "lteq", "lt"}
_SYMBOL_TO_OPERATOR = {
    ">": "gt",
    ">=": "gteq",
    "=": "eq",
    "<=": "lteq",
    "<": "lt",
}

_SYMBOL_FILTER_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|<=|=|>|<)\s*(.+?)\s*$")
_WORD_FILTER_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+(gt|gteq|eq|lteq|lt)\s+(.+?)\s*$"
)


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
        "filter: Invalid criteria. Use 'data_identifier>=value' "
        "(operators: >,>=,=,<=,<)."
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
        group = in_state.lower()
    elif in_county:
        group = in_county
    elif in_zcta:
        group = in_zcta

    if universe and group:
        return f"{universe}+{group}"
    if universe:
        return f"{universe}+"
    if group:
        return group
    return ""

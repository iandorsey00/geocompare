import pytest

from geocompare.tools.query_syntax import build_context, parse_geofilter


def test_parse_geofilter_supports_legacy_syntax():
    filters = parse_geofilter("population:gteq:100000")
    assert filters == [
        {"comp": "population", "operator": "gteq", "value": "100000", "data_type": None}
    ]


def test_parse_geofilter_supports_symbol_syntax():
    filters = parse_geofilter("population>=100000")
    assert filters == [
        {"comp": "population", "operator": "gteq", "value": "100000", "data_type": None}
    ]


def test_parse_geofilter_supports_multiple_criteria():
    filters = parse_geofilter("population>=100000,median_household_income:gt:90000")
    assert len(filters) == 2
    assert filters[0]["operator"] == "gteq"
    assert filters[1]["operator"] == "gt"


def test_parse_geofilter_rejects_invalid_criteria():
    with pytest.raises(ValueError):
        parse_geofilter("population@100000")


def test_build_context_supports_explicit_scope_args():
    context = build_context(universe="places", in_state="CA")
    assert context == "places+ca"


def test_build_context_rejects_mixed_legacy_and_explicit_scope():
    with pytest.raises(ValueError):
        build_context(context="places+ca", universe="places")

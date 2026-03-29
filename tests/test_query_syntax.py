import pytest

from geocompare.tools.query_syntax import build_context, parse_geofilter
from geocompare.tools.summary_level_parser import SummaryLevelParser


def test_parse_geofilter_supports_word_operator_syntax():
    filters = parse_geofilter("population gteq 100000")
    assert filters == [{"comp": "population", "operator": "gteq", "value": "100000"}]


def test_parse_geofilter_supports_symbol_syntax():
    filters = parse_geofilter("population>=100000")
    assert filters == [{"comp": "population", "operator": "gteq", "value": "100000"}]


def test_parse_geofilter_supports_multiple_criteria():
    filters = parse_geofilter("population>=100000,median_household_income>90000")
    assert len(filters) == 2
    assert filters[0]["operator"] == "gteq"
    assert filters[1]["operator"] == "gt"


def test_parse_geofilter_rejects_invalid_criteria():
    with pytest.raises(ValueError):
        parse_geofilter("population@100000")


def test_parse_geofilter_rejects_legacy_data_type_suffix():
    with pytest.raises(ValueError):
        parse_geofilter("population>=100000:c")


def test_build_context_supports_explicit_scope_args():
    context = build_context(universe="places", in_state="CA")
    assert context == "places+ca"


def test_build_context_supports_full_state_name():
    context = build_context(universe="places", in_state="Minnesota")
    assert context == "places+mn"


def test_build_context_supports_us_as_state_group():
    context = build_context(universe="places", in_state="US")
    assert context == "places+us"


def test_build_context_rejects_mixed_legacy_and_explicit_scope():
    with pytest.raises(ValueError):
        build_context(context="places+ca", universe="places")


def test_summary_level_parser_parses_multiple_universes():
    parser = SummaryLevelParser()
    assert parser.parse_universes("place,tracts") == ["160", "140"]


def test_summary_level_parser_parses_friendly_universe_names():
    parser = SummaryLevelParser()
    assert parser.parse_universes("Places, Census Tracts") == ["160", "140"]


def test_summary_level_parser_parses_all_universes():
    parser = SummaryLevelParser()
    assert parser.parse_universes("All") == ["010", "050", "040", "140", "160", "310", "400", "860"]

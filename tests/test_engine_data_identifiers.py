from types import SimpleNamespace

import pytest

from geocompare.engine import Engine


def _fetch_one_stub():
    return SimpleNamespace(
        rc={"population": 100, "bachelors_degree_or_higher": 40, "democratic_voters_pct": 52.0},
        c={"population_density": 123.4, "bachelors_degree_or_higher": 40.0},
        rh={
            "population": "Total population",
            "population_density": "Population density",
            "bachelors_degree_or_higher": "Bachelor's degree or higher",
            "democratic_voters_pct": "Democratic voters (%)",
        },
    )


def test_resolve_data_identifier_prefers_raw_for_shared_keys():
    engine = Engine.__new__(Engine)
    resolved = engine.resolve_data_identifier("bachelors_degree_or_higher", _fetch_one_stub())
    assert resolved["store"] == "rc"
    assert resolved["display_store"] == "fc"
    assert resolved["key"] == "bachelors_degree_or_higher"


def test_resolve_data_identifier_supports_pct_suffix_for_compounds():
    engine = Engine.__new__(Engine)
    resolved = engine.resolve_data_identifier("bachelors_degree_or_higher_pct", _fetch_one_stub())
    assert resolved["store"] == "c"
    assert resolved["display_store"] == "fcd"
    assert resolved["key"] == "bachelors_degree_or_higher"


def test_resolve_data_identifier_uses_pct_identifier_when_present_in_raw_metrics():
    engine = Engine.__new__(Engine)
    resolved = engine.resolve_data_identifier("democratic_voters_pct", _fetch_one_stub())
    assert resolved["store"] == "rc"
    assert resolved["display_store"] == "fc"
    assert resolved["key"] == "democratic_voters_pct"


def test_resolve_data_identifier_rejects_unknown_identifier():
    engine = Engine.__new__(Engine)
    with pytest.raises(ValueError):
        engine.resolve_data_identifier("democratic_voters_pc", _fetch_one_stub())

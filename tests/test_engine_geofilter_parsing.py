from types import SimpleNamespace

from geocompare.engine import Engine
from geocompare.tools.summary_level_parser import SummaryLevelParser


def _engine_stub():
    engine = Engine.__new__(Engine)
    engine.get_data_types = lambda comp, data_type, fetch_one: ("rc", "fc")
    engine.slt = SummaryLevelParser()
    return engine


def _profile(population):
    return SimpleNamespace(
        sumlevel="160",
        state="ca",
        name=f"Place {population}",
        counties=[],
        rc={"population": population},
    )


def test_build_sql_conditions_accepts_symbol_operator():
    engine = _engine_stub()
    fetch_one = _profile(0)
    conditions = engine._build_sql_geofilter_conditions("population>=100000", fetch_one)
    assert conditions == [{"column": "rc_population", "operator": "gteq", "value": 100000}]


def test_context_filter_accepts_symbol_operator():
    engine = _engine_stub()
    profiles = [_profile(50000), _profile(100000), _profile(150000)]
    filtered = engine.context_filter(profiles, context="", geofilter="population>=100000")
    assert [profile.rc["population"] for profile in filtered] == [100000, 150000]

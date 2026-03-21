from types import SimpleNamespace

import pytest

from geocompare.engine import Engine
from geocompare.tools.summary_level_parser import SummaryLevelParser


def _profile(name, population, income, latitude, longitude):
    return SimpleNamespace(
        name=name,
        sumlevel="140",
        state="ca",
        counties=["06075"],
        rc={
            "population": population,
            "median_household_income": income,
            "latitude": latitude,
            "longitude": longitude,
        },
        c={},
        fc={
            "population": f"{population:,}",
            "median_household_income": f"${income:,}",
        },
        fcd={},
        rh={
            "population": "Total population",
            "median_household_income": "Median household income",
        },
    )


def _engine_with_profiles(profiles):
    engine = Engine.__new__(Engine)
    engine.d = {"demographicprofiles": profiles}
    engine.slt = SummaryLevelParser()
    engine.resolve_data_identifier = lambda comp, fetch_one: {
        "store": "rc",
        "key": comp,
        "display_store": "fc",
        "label": fetch_one.rh.get(comp, comp),
    }
    engine.get_data_products = lambda: engine.d
    return engine


def test_remoteness_ranks_candidates_by_distance_to_nearest_below_threshold():
    profiles = [
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4500, 90000, 0.0, 10.0),
        _profile("Tract C", 4000, 60000, 0.0, 1.0),
        _profile("Tract D", 4200, 50000, 0.0, 15.0),
    ]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness("median_household_income", 75000, context="tracts+", n=10)

    assert [row["candidate"].name for row in results] == ["Tract B", "Tract A"]
    assert results[0]["nearest_match"].name == "Tract D"
    assert results[1]["nearest_match"].name == "Tract C"
    assert round(results[0]["distance_miles"], 1) > round(results[1]["distance_miles"], 1)


def test_remoteness_applies_geofilter_to_population_screen():
    profiles = [
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 500, 55000, 0.0, 0.1),
        _profile("Tract C", 4500, 60000, 0.0, 1.0),
    ]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness(
        "median_household_income",
        75000,
        context="tracts+",
        geofilter="population>=1000",
        n=10,
    )

    assert len(results) == 1
    assert results[0]["candidate"].name == "Tract A"
    assert results[0]["nearest_match"].name == "Tract C"


def test_remoteness_rejects_missing_qualifying_side():
    profiles = [
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4500, 90000, 0.0, 1.0),
    ]
    engine = _engine_with_profiles(profiles)

    with pytest.raises(ValueError):
        engine.remoteness("median_household_income", 75000, context="tracts+", n=10)

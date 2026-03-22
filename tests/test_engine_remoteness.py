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
        geoid=f"1400000US06075{name[-1] if name[-1].isdigit() else '000000'}",
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


def _county_profile(name, county_geoid, population, density):
    return SimpleNamespace(
        name=name,
        sumlevel="050",
        state="ca",
        counties=[county_geoid],
        geoid=f"05000US{county_geoid}",
        rc={
            "population": population,
            "latitude": 0.0,
            "longitude": 0.0,
        },
        c={"population_density": density},
        fc={"population": f"{population:,}"},
        fcd={"population_density": f"{density:,.1f}/sqmi"},
        rh={
            "population": "Total population",
            "population_density": "Population density",
        },
    )


def _engine_with_profiles(profiles):
    engine = Engine.__new__(Engine)
    engine.d = {"demographicprofiles": profiles}
    engine.ct = SimpleNamespace(
        county_name_to_geoid={
            "Big County, California": "06001",
            "Small County, California": "06013",
        }
    )
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


def test_remoteness_applies_geofilter_to_candidates_only_by_default():
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
    assert results[0]["nearest_match"].name == "Tract B"


def test_remoteness_can_filter_qualifying_side_separately():
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
        match_geofilter="population>=1000",
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


def test_remoteness_can_filter_to_large_counties():
    profiles = [
        _county_profile("Big County, California", "06001", 1_500_000, 1200.0),
        _county_profile("Small County, California", "06013", 150_000, 150.0),
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4500, 90000, 0.0, 10.0),
        _profile("Tract C", 4000, 60000, 0.0, 5.0),
        _profile("Tract D", 4200, 50000, 0.0, 15.0),
    ]
    profiles[2].counties = ["06001"]
    profiles[3].counties = ["06013"]
    profiles[4].counties = ["06001"]
    profiles[5].counties = ["06013"]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness(
        "median_household_income",
        75000,
        context="tracts+",
        county_population_min=1_000_000,
        n=10,
    )

    assert len(results) == 1
    assert results[0]["candidate"].name == "Tract A"
    assert results[0]["nearest_match"].name == "Tract C"


def test_remoteness_applies_county_filters_to_candidates_only():
    profiles = [
        _county_profile("Big County, California", "06001", 1_500_000, 1200.0),
        _county_profile("Small County, California", "06013", 150_000, 150.0),
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4200, 60000, 0.0, 1.0),
        _profile("Tract C", 4000, 55000, 0.0, 5.0),
    ]
    profiles[2].counties = ["06001"]
    profiles[3].counties = ["06013"]
    profiles[4].counties = ["06001"]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness(
        "median_household_income",
        75000,
        context="tracts+",
        county_population_min=1_000_000,
        n=10,
    )

    assert len(results) == 1
    assert results[0]["candidate"].name == "Tract A"
    assert results[0]["nearest_match"].name == "Tract B"


def test_remoteness_can_filter_to_dense_counties():
    profiles = [
        _county_profile("Big County, California", "06001", 1_500_000, 1200.0),
        _county_profile("Small County, California", "06013", 1_800_000, 400.0),
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4500, 90000, 0.0, 10.0),
        _profile("Tract C", 4000, 60000, 0.0, 1.0),
        _profile("Tract D", 4200, 50000, 0.0, 15.0),
    ]
    profiles[2].counties = ["06001"]
    profiles[3].counties = ["06013"]
    profiles[4].counties = ["06001"]
    profiles[5].counties = ["06013"]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness(
        "median_household_income",
        75000,
        context="tracts+",
        county_density_min=1000.0,
        n=10,
    )

    assert len(results) == 1
    assert results[0]["candidate"].name == "Tract A"
    assert results[0]["nearest_match"].name == "Tract C"


def test_remoteness_can_limit_to_one_result_per_county():
    profiles = [
        _profile("Tract A", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", 4500, 95000, 0.0, 10.0),
        _profile("Tract C", 4000, 60000, 0.0, 1.0),
        _profile("Tract D", 4200, 50000, 0.0, 15.0),
        _profile("Tract E", 4300, 92000, 0.0, 20.0),
        _profile("Tract F", 4100, 55000, 0.0, 21.0),
    ]
    profiles[0].counties = ["06001"]
    profiles[1].counties = ["06001"]
    profiles[2].counties = ["06001"]
    profiles[3].counties = ["06013"]
    profiles[4].counties = ["06013"]
    profiles[5].counties = ["06013"]
    engine = _engine_with_profiles(profiles)

    results = engine.remoteness(
        "median_household_income",
        75000,
        context="tracts+",
        one_per_county=True,
        n=10,
    )

    assert [row["candidate"].name for row in results] == ["Tract B", "Tract E"]

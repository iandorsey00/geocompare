from types import SimpleNamespace

from geocompare.engine import Engine
from geocompare.tools.summary_level_parser import SummaryLevelParser


def _profile(name, county_geoid, population, income, latitude, longitude):
    return SimpleNamespace(
        name=name,
        sumlevel="140",
        state="ca",
        counties=[county_geoid],
        geoid=f"1400000US{county_geoid}{name[-1] if name[-1].isdigit() else '000000'}",
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
    engine._dp_by_name = {dp.name: dp for dp in profiles}
    engine.ct = SimpleNamespace(
        county_name_to_geoid={
            "County One, California": "06001",
            "County Two, California": "06013",
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


def test_local_average_ranks_high_income_cluster_above_low_income_cluster():
    profiles = [
        _profile("Tract A", "06001", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", "06001", 5000, 95000, 0.0, 1.0),
        _profile("Tract C", "06013", 5000, 20000, 0.0, 10.0),
        _profile("Tract D", "06013", 5000, 15000, 0.0, 11.0),
    ]
    engine = _engine_with_profiles(profiles)

    results = engine.local_average(
        "median_household_income",
        context="tracts+",
        neighbors=2,
        n=10,
    )

    assert [row["candidate"].name for row in results[:4]] == [
        "Tract B",
        "Tract A",
        "Tract D",
        "Tract C",
    ]
    assert results[0]["local_average"] > results[1]["local_average"] > results[2]["local_average"]


def test_local_average_can_limit_to_one_result_per_county():
    profiles = [
        _profile("Tract A", "06001", 5000, 100000, 0.0, 0.0),
        _profile("Tract B", "06001", 5000, 95000, 0.0, 1.0),
        _profile("Tract C", "06013", 5000, 20000, 0.0, 10.0),
        _profile("Tract D", "06013", 5000, 15000, 0.0, 11.0),
    ]
    engine = _engine_with_profiles(profiles)

    results = engine.local_average(
        "median_household_income",
        context="tracts+",
        neighbors=2,
        one_per_county=True,
        n=10,
    )

    assert [row["candidate"].name for row in results] == ["Tract B", "Tract D"]

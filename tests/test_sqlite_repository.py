import sqlite3
import zlib
from types import SimpleNamespace

from geocompare.engine import Engine
from geocompare.repository.serialization import dump_payload
from geocompare.repository.sqlite_repository import (
    CURRENT_SCHEMA_VERSION,
    SQLiteRepository,
    _COMPRESSED_PAYLOAD_PREFIX,
)


class DummyProfile:
    def __init__(self, name, state, sumlevel, geoid, population):
        self.name = name
        self.state = state
        self.sumlevel = sumlevel
        self.geoid = geoid
        self.counties = []
        self.rc = {
            "population": population,
            "latitude": 37.0,
            "longitude": -122.0,
        }
        self.c = {
            "population_density": 10.0,
        }


class DummyGeoVector:
    def __init__(self, name, state, sumlevel):
        self.name = name
        self.state = state
        self.sumlevel = sumlevel


def _products():
    return {
        "demographicprofiles": [
            DummyProfile("Alpha city, California", "ca", "160", "16000US0601000", 1000),
            DummyProfile("Beta city, California", "ca", "160", "16000US0602000", 2000),
        ],
        "geovectors": [
            DummyGeoVector("Alpha city, California", "ca", "160"),
            DummyGeoVector("Beta city, California", "ca", "160"),
        ],
    }


def test_schema_version_table_created(tmp_path):
    repo = SQLiteRepository(tmp_path / "test.sqlite")
    repo.save_data_products(_products())

    conn = sqlite3.connect(str(tmp_path / "test.sqlite"))
    try:
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == CURRENT_SCHEMA_VERSION


def test_roundtrip_load_products(tmp_path):
    repo = SQLiteRepository(tmp_path / "test.sqlite")
    repo.save_data_products(_products())

    loaded = repo.load_data_products()
    assert len(loaded["demographicprofiles"]) == 2
    assert len(loaded["geovectors"]) == 2


def test_get_demographic_profile_supports_compressed_payloads(tmp_path):
    repo = SQLiteRepository(tmp_path / "test.sqlite")
    repo.save_data_products(_products())

    conn = sqlite3.connect(str(tmp_path / "test.sqlite"))
    try:
        compressed_payload = _COMPRESSED_PAYLOAD_PREFIX + zlib.compress(
            dump_payload(_products()["demographicprofiles"][0]),
            level=6,
        )
        conn.execute(
            "UPDATE demographic_profiles SET payload = ? WHERE name = ?",
            (compressed_payload, "Alpha city, California"),
        )
        conn.commit()
    finally:
        conn.close()

    profile = repo.get_demographic_profile("Alpha city, California")
    assert profile is not None
    assert profile.name == "Alpha city, California"


def test_get_demographic_profile_by_geoid_returns_match(tmp_path):
    repo = SQLiteRepository(tmp_path / "test.sqlite")
    repo.save_data_products(_products())

    profile = repo.get_demographic_profile_by_geoid("16000US0601000")
    assert profile is not None
    assert profile.name == "Alpha city, California"


def test_engine_get_dp_uses_repository_before_loading_all_data():
    engine = Engine()
    engine.primary_repository = SimpleNamespace(
        get_demographic_profile=lambda name: SimpleNamespace(name=name),
    )
    engine._repo_supports = lambda method: method == "get_demographic_profile"
    engine.get_data_products = lambda: (_ for _ in ()).throw(AssertionError("should not load all data"))

    profile = engine.get_dp("Alpha city, California")[0]
    assert profile.name == "Alpha city, California"


def test_engine_fetch_profile_by_geoid_uses_repository_before_loading_all_data():
    engine = Engine()
    engine.primary_repository = SimpleNamespace(
        get_demographic_profile_by_geoid=lambda geoid: SimpleNamespace(name="Alpha city, California", geoid=geoid),
    )
    engine._repo_supports = lambda method: method == "get_demographic_profile_by_geoid"
    engine.get_data_products = lambda: (_ for _ in ()).throw(AssertionError("should not load all data"))

    profile = engine._fetch_profile_by_geoid("16000US0601000")
    assert profile.name == "Alpha city, California"
    assert profile.geoid == "16000US0601000"


def test_engine_closest_geographies_uses_repository_before_loading_all_data():
    engine = Engine()
    target_profile = SimpleNamespace(
        name="Alpha city, California",
        rc={"latitude": 37.0, "longitude": -122.0},
    )
    beta_profile = SimpleNamespace(name="Beta city, California")
    gamma_profile = SimpleNamespace(name="Gamma city, California")
    queried_names = []

    engine._lookup_dp = lambda name: (
        target_profile
        if name == "Alpha city, California"
        else queried_names.append(name) or {
            "Beta city, California": beta_profile,
            "Gamma city, California": gamma_profile,
        }[name]
    )
    engine._build_sql_query_params = lambda context, geofilter, fetch_one: {
        "universe_sl": "160",
        "group_sl": None,
        "group": None,
        "county_geoid": None,
        "geofilter_conditions": [],
    }
    engine.primary_repository = SimpleNamespace(
        query_profile_coordinates=lambda **kwargs: [
            ("Beta city, California", 37.1, -122.1),
            ("Gamma city, California", 37.2, -122.2),
        ],
    )
    engine._repo_supports = lambda method: method == "query_profile_coordinates"
    engine.get_data_products = lambda: (_ for _ in ()).throw(AssertionError("should not load all data"))

    rows = engine.closest_geographies("Alpha city, California", context="places+", n=2)
    assert [profile.name for profile, _distance in rows] == [
        "Beta city, California",
        "Gamma city, California",
    ]
    assert queried_names == ["Beta city, California", "Gamma city, California"]

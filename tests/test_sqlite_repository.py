import sqlite3
import zlib

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

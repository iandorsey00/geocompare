from pathlib import Path

from geocompare.database.Database import Database


class FakeDP:
    def __init__(self, geoid, population):
        self.geoid = geoid
        self.rc = {"population": population}
        self.added = []

    def add_custom_metric(self, **kwargs):
        self.added.append(kwargs)


def test_detect_latest_years(tmp_path):
    data_dir = Path(tmp_path)
    (data_dir / "g20215us.csv").touch()
    (data_dir / "g20245us.csv").touch()
    (data_dir / "2023_Gaz_place_national.txt").touch()
    (data_dir / "2025_Gaz_place_national.txt").touch()

    db = Database.__new__(Database)
    assert db.detect_latest_acs_year(data_dir) == "2024"
    assert db.detect_latest_gazetteer_year(data_dir) == "2025"


def test_detect_latest_acs_year_from_txt(tmp_path):
    data_dir = Path(tmp_path)
    (data_dir / "g20205us.txt").touch()
    (data_dir / "g20245us.txt").touch()

    db = Database.__new__(Database)
    assert db.detect_latest_acs_year(data_dir) == "2024"


def test_detect_latest_acs_year_from_table_based_geos(tmp_path):
    data_dir = Path(tmp_path)
    (data_dir / "Geos20235YR.txt").touch()
    (data_dir / "Geos20245YR.txt").touch()

    db = Database.__new__(Database)
    assert db.detect_latest_acs_year(data_dir) == "2024"


def test_apply_overlays_adds_crime_and_project_metrics():
    dp = FakeDP("16000US0601000", 1000)

    db = Database.__new__(Database)
    db.demographicprofiles = [dp]
    db.overlays = {
        "0601000": {
            "violent_crime_count": 50.0,
            "custom_index": 0.382,
        }
    }

    db.apply_overlays()

    added_keys = {row["key"] for row in dp.added}
    assert "violent_crime_count" in added_keys
    assert "project_custom_index" in added_keys

    crime_metric = next(row for row in dp.added if row["key"] == "violent_crime_count")
    assert crime_metric["section_title"] == "CRIME"
    assert crime_metric["compound_display"].endswith("/100k")

    project_metric = next(row for row in dp.added if row["key"] == "project_custom_index")
    assert project_metric["section_title"] == "PROJECT DATA"


def test_apply_overlays_derives_crime_rates_from_counts():
    dp = FakeDP("16000US0601000", 2000)

    db = Database.__new__(Database)
    db.demographicprofiles = [dp]
    db.overlays = {
        "0601000": {
            "violent_crime_count": 50.0,
            "property_crime_count": 150.0,
            "total_crime_count": 200.0,
        }
    }

    db.apply_overlays()

    added = {row["key"]: row for row in dp.added}

    assert "violent_crime_rate" in added
    assert "property_crime_rate" in added
    assert "total_crime_rate" in added

    assert round(added["violent_crime_rate"]["value"], 1) == 2500.0
    assert round(added["property_crime_rate"]["value"], 1) == 7500.0
    assert round(added["total_crime_rate"]["value"], 1) == 10000.0


def test_apply_overlays_adds_voter_metrics_and_pct_values():
    dp = FakeDP("16000US0601000", 2000)

    db = Database.__new__(Database)
    db.demographicprofiles = [dp]
    db.overlays = {
        "0601000": {
            "registered_voters": 1000.0,
            "democratic_voters": 520.0,
            "republican_voters": 380.0,
            "other_voters": 100.0,
        }
    }

    db.apply_overlays()

    added = {row["key"]: row for row in dp.added}
    assert "registered_voters" in added
    assert "democratic_voters_pct" in added
    assert "republican_voters_pct" in added
    assert "other_voters_pct" in added

    assert added["registered_voters"]["section_title"] == "CIVICS"
    assert added["registered_voters"]["value_display"] == "1,000"
    assert round(added["democratic_voters_pct"]["value"], 1) == 52.0
    assert round(added["republican_voters_pct"]["value"], 1) == 38.0
    assert round(added["other_voters_pct"]["value"], 1) == 10.0
    assert added["democratic_voters_pct"]["value_display"].endswith("%")


def test_apply_overlays_deduplicates_full_geoid_matches():
    dp = FakeDP("16000US0665042", 1000)

    db = Database.__new__(Database)
    db.demographicprofiles = [dp]
    db.overlays = {
        "1600000US0665042": {
            "registered_voters": 100.0,
            "democratic_voters": 55.0,
            "republican_voters": 30.0,
            "other_voters": 15.0,
        }
    }

    db.apply_overlays()

    registered_rows = [row for row in dp.added if row["key"] == "registered_voters"]
    percent_dem_rows = [row for row in dp.added if row["key"] == "democratic_voters_pct"]

    assert len(registered_rows) == 1
    assert len(percent_dem_rows) == 1


def test_detect_acs_layout(tmp_path):
    data_dir = Path(tmp_path)
    (data_dir / "Geos20245YR.txt").touch()
    (data_dir / "g20245us.csv").touch()

    db = Database.__new__(Database)
    assert db.detect_acs_layout(data_dir, "2024") == "table"

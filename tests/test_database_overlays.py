from pathlib import Path

from geodata.database.Database import Database


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


def test_apply_overlays_adds_crime_and_project_metrics():
    dp = FakeDP("16000US0601000", 1000)

    db = Database.__new__(Database)
    db.demographicprofiles = [dp]
    db.overlays = {
        "0601000": {
            "violent_crime_count": 50.0,
            "social_alignment_index": 0.382,
        }
    }

    db.apply_overlays()

    added_keys = {row["key"] for row in dp.added}
    assert "violent_crime_count" in added_keys
    assert "project_social_alignment_index" in added_keys

    crime_metric = next(row for row in dp.added if row["key"] == "violent_crime_count")
    assert crime_metric["section_title"] == "CRIME"
    assert crime_metric["compound_display"].endswith("/100k")

    project_metric = next(row for row in dp.added if row["key"] == "project_social_alignment_index")
    assert project_metric["section_title"] == "PROJECT DATA"

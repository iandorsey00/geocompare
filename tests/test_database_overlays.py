from pathlib import Path

from geocompare.database.Database import Database
from geocompare.models.demographic_profile import DemographicProfile


class FakeDP:
    def __init__(self, geoid, population, sumlevel="160"):
        self.geoid = geoid
        self.sumlevel = sumlevel
        self.rc = {"population": population}
        self.added = []

    def add_custom_metric(self, **kwargs):
        self.rc[kwargs["key"]] = kwargs["value"]
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
    assert crime_metric["label"] == "Violent crimes"
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


def test_apply_overlays_adds_voter_metrics_with_inline_percentages():
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

    assert added["registered_voters"]["section_title"] == "VOTER REGISTRATION"
    assert added["registered_voters"]["value_display"] == "1,000"
    assert added["registered_voters"]["compound_display"] == "50.0%"
    assert added["registered_voters"]["indent"] == 0
    assert added["democratic_voters"]["indent"] == 2
    assert added["republican_voters"]["indent"] == 2
    assert added["other_voters"]["indent"] == 2
    assert added["democratic_voters"]["compound_display"] == "52.0%"
    assert added["republican_voters"]["compound_display"] == "38.0%"
    assert round(added["democratic_voters_pct"]["value"], 1) == 52.0
    assert round(added["republican_voters_pct"]["value"], 1) == 38.0
    assert round(added["other_voters_pct"]["value"], 1) == 10.0
    assert added["democratic_voters_pct"]["value_display"].endswith("%")

    visible_voter_keys = [
        row["key"]
        for row in dp.added
        if row["section_title"] == "VOTER REGISTRATION" and row["show_in_profile"]
    ]
    assert visible_voter_keys[:4] == [
        "registered_voters",
        "democratic_voters",
        "republican_voters",
        "other_voters",
    ]


def test_voter_overlay_rows_sort_registered_then_party_breakout():
    dp = DemographicProfile.__new__(DemographicProfile)
    dp.rh = {
        "registered_voters": "Registered voters",
        "democratic_voters": "  Democratic voters",
        "republican_voters": "  Republican voters",
        "other_voters": "  Other voters",
    }

    db = Database.__new__(Database)
    ordered = sorted(
        [
            ("std", "other_voters"),
            ("std", "registered_voters"),
            ("std", "republican_voters"),
            ("std", "democratic_voters"),
        ],
        key=lambda row: db._overlay_row_sort_key(dp, row),
    )

    assert [key for _mode, key in ordered] == [
        "registered_voters",
        "democratic_voters",
        "republican_voters",
        "other_voters",
    ]


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


def test_apply_overlays_respects_geo_level_county_vs_zcta_collision():
    county_dp = FakeDP("0500000US01003", 200000, sumlevel="050")
    zcta_dp = FakeDP("8600000US01003", 5000, sumlevel="860")

    db = Database.__new__(Database)
    db.demographicprofiles = [county_dp, zcta_dp]
    db.overlays = {
        "01003": {
            "social_geo_level_code": 860.0,
            "social_ai_score": 70.0,
        }
    }

    db.apply_overlays()

    county_keys = {row["key"] for row in county_dp.added}
    zcta_keys = {row["key"] for row in zcta_dp.added}
    assert "project_social_ai_score" not in county_keys
    assert "project_social_ai_score" in zcta_keys


def test_apply_overlays_respects_geo_level_place_vs_county_collision():
    place_dp = FakeDP("1600000US01003", 12000, sumlevel="160")
    county_dp = FakeDP("0500000US01003", 200000, sumlevel="050")

    db = Database.__new__(Database)
    db.demographicprofiles = [place_dp, county_dp]
    db.overlays = {
        "01003": {
            "social_geo_level_code": 50.0,
            "social_ai_score": 42.0,
        }
    }

    db.apply_overlays()

    place_keys = {row["key"] for row in place_dp.added}
    county_keys = {row["key"] for row in county_dp.added}
    assert "project_social_ai_score" not in place_keys
    assert "project_social_ai_score" in county_keys


def test_apply_overlays_legacy_without_geo_level_code_still_matches():
    place_dp = FakeDP("1600000US01003", 12000, sumlevel="160")
    county_dp = FakeDP("0500000US01003", 200000, sumlevel="050")

    db = Database.__new__(Database)
    db.demographicprofiles = [place_dp, county_dp]
    db.overlays = {
        "01003": {
            "social_ai_score": 13.5,
        }
    }

    db.apply_overlays()

    place_keys = {row["key"] for row in place_dp.added}
    county_keys = {row["key"] for row in county_dp.added}
    assert "project_social_ai_score" in place_keys
    assert "project_social_ai_score" in county_keys


def test_apply_overlays_keeps_duplicate_geoid_rows_across_levels():
    county_dp = FakeDP("0500000US01003", 200000, sumlevel="050")
    zcta_dp = FakeDP("8600000US01003", 5000, sumlevel="860")

    db = Database.__new__(Database)
    db.demographicprofiles = [county_dp, zcta_dp]
    db.overlays = {
        "01003__sl050": {
            "social_geo_level_code": 50.0,
            "social_ai_score": 18.0,
        },
        "01003__sl860": {
            "social_geo_level_code": 860.0,
            "social_ai_score": 57.0,
        },
    }

    db.apply_overlays()

    county_score = next(row for row in county_dp.added if row["key"] == "project_social_ai_score")
    zcta_score = next(row for row in zcta_dp.added if row["key"] == "project_social_ai_score")
    assert county_score["value"] == 18.0
    assert zcta_score["value"] == 57.0


def test_detect_acs_layout(tmp_path):
    data_dir = Path(tmp_path)
    (data_dir / "Geos20245YR.txt").touch()
    (data_dir / "g20245us.csv").touch()

    db = Database.__new__(Database)
    assert db.detect_acs_layout(data_dir, "2024") == "table"

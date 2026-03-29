from geocompare.models.geovector import GeoVector


def _base_row():
    return {
        "SUMLEVEL": "160",
        "STUSAB": "CA",
        "GEOID": "12345",
        "NAME": "Example city, California",
        "B01003_1": "10000",
        "ALAND_SQMI": "10",
        "B19301_1": "50000",
        "B02001_2": "5000",
        "B02001_3": "1000",
        "B02001_5": "2000",
        "B03002_12": "3000",
        "B15003_1": "7000",
        "B15003_22": "1000",
        "B15003_23": "700",
        "B15003_24": "200",
        "B15003_25": "100",
        "B25035_1": "1985",
        "B25010_1": "2.8",
        "B25003_1": "3800",
        "B25003_2": "2500",
        "B25018_1": "5.5",
    }


def _medians():
    return {
        "B01003_1": "8000",
        "ALAND_SQMI": "8",
        "B19301_1": "45000",
        "B02001_2": "3200",
        "B02001_3": "900",
        "B02001_5": "1600",
        "B03002_12": "800",
        "B15003_1": "6000",
        "B15003_22": "900",
        "B15003_23": "500",
        "B15003_24": "150",
        "B15003_25": "80",
        "B25035_1": "1978",
        "B25010_1": "2.5",
        "B25003_1": "3200",
        "B25003_2": "1800",
        "B25018_1": "5.0",
    }


def _standard_deviations():
    return {
        "B01003_1": "2000",
        "ALAND_SQMI": "2",
        "B19301_1": "9000",
        "B02001_2": "700",
        "B02001_3": "200",
        "B02001_5": "300",
        "B03002_12": "100",
        "B15003_1": "1000",
        "B15003_22": "200",
        "B15003_23": "120",
        "B15003_24": "40",
        "B15003_25": "20",
        "B25035_1": "10",
        "B25010_1": "0.3",
        "B25003_1": "500",
        "B25003_2": "400",
        "B25018_1": "0.5",
    }


def test_geovector_uses_asian_field_for_asian_share_statistics():
    gv = GeoVector(_base_row(), _medians(), _standard_deviations())

    assert gv.med["asian_alone"] == 20.0
    assert gv.med["hispanic_or_latino"] == 10.0
    assert gv.sd["asian_alone"] == 15.0
    assert gv.sd["asian_alone"] != gv.sd["hispanic_or_latino"]


def test_geovector_built_form_mode_uses_housing_and_form_dimensions():
    gv = GeoVector(_base_row(), _medians(), _standard_deviations())

    assert set(gv.ws["app"].keys()) == {
        "population_density",
        "housing_density",
        "homeowner_occupied_housing_units",
        "median_year_structure_built",
        "median_rooms",
        "average_household_size",
    }
    assert "per_capita_income" not in gv.ws["app"]


def test_geovector_form_mode_alias_matches_app_mode_distance():
    gv1 = GeoVector(_base_row(), _medians(), _standard_deviations())
    row2 = _base_row()
    row2["B25003_2"] = "1800"
    row2["B25018_1"] = "4.8"
    row2["B25010_1"] = "2.4"
    row2["B25035_1"] = "1970"
    gv2 = GeoVector(row2, _medians(), _standard_deviations())

    assert gv1.distance(gv2, mode="form") == gv1.distance(gv2, mode="app")

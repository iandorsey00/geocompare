from geocompare.tools.geography_names import (
    county_display_names,
    county_geoids_for_geography,
    format_tract_code,
    tract_display_name_from_geoid,
)


def test_format_tract_code_handles_whole_and_fractional_codes():
    assert format_tract_code("060100") == "601"
    assert format_tract_code("060101") == "601.01"


def test_tract_display_name_from_geoid_uses_county_and_state():
    assert (
        tract_display_name_from_geoid("14000US06075060100")
        == "Census Tract 601, San Francisco County, California"
    )


def test_county_lookup_helpers_support_tracts():
    assert county_geoids_for_geography("14000US06075060100", "140") == ["06075"]
    assert county_display_names(["06075"]) == ["San Francisco County"]

from geocompare.tools.geography_names import (
    compact_place_name,
    county_display_names,
    county_geoids_for_geography,
    format_tract_code,
    humanized_tract_name,
    tract_display_name_from_geoid,
)


def test_format_tract_code_handles_whole_and_fractional_codes():
    assert format_tract_code("060100") == "601"
    assert format_tract_code("060101") == "601.01"


def test_tract_display_name_from_geoid_uses_county_and_state():
    assert (
        tract_display_name_from_geoid("1400000US06075060100")
        == "Census Tract 601, San Francisco County, California"
    )


def test_county_lookup_helpers_support_tracts():
    assert county_geoids_for_geography("1400000US06075060100", "140") == ["06075"]
    assert county_display_names(["06075"]) == ["San Francisco County"]


def test_county_lookup_helpers_support_counties():
    assert county_geoids_for_geography("0500000US06075", "050") == ["06075"]
    assert county_display_names(["06075"]) == ["San Francisco County"]


def test_compact_place_name_strips_census_suffix():
    assert compact_place_name("Pahrump CDP, Nevada") == "Pahrump"


def test_humanized_tract_name_uses_nearby_place_and_state_abbrev():
    assert (
        humanized_tract_name(
            "1400000US32023960100",
            nearby_place_name="Pahrump CDP, Nevada",
            state_abbrev="nv",
        )
        == "9601, near Pahrump, Nye County, NV"
    )

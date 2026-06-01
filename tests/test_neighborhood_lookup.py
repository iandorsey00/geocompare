import json

from geocompare.tools.neighborhood_lookup import NeighborhoodLookup


def test_neighborhood_lookup_matches_point_inside_polygon(tmp_path):
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir()
    geojson_path = reference_dir / "neighborhoods.geojson"
    geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "Koreatown",
                            "city": "Los Angeles city, California",
                            "state": "ca",
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-118.31, 34.05],
                                    [-118.27, 34.05],
                                    [-118.27, 34.08],
                                    [-118.31, 34.08],
                                    [-118.31, 34.05],
                                ]
                            ],
                        },
                    }
                ],
            }
        )
    )

    lookup = NeighborhoodLookup.from_data_dir(tmp_path)
    match = lookup.match(
        34.061, -118.295, city_name="Los Angeles city, California", state_abbrev="ca"
    )

    assert match is not None
    assert match["name"] == "Koreatown"
    assert match["city"] == "Los Angeles city, California"


def test_neighborhood_lookup_prefers_matching_city_when_multiple_contain_point(tmp_path):
    reference_dir = tmp_path / "reference"
    reference_dir.mkdir()
    geojson_path = reference_dir / "neighborhoods.geojson"
    geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "Generic Area",
                            "city": "West Hollywood city, California",
                            "state": "ca",
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-118.32, 34.04],
                                    [-118.26, 34.04],
                                    [-118.26, 34.09],
                                    [-118.32, 34.09],
                                    [-118.32, 34.04],
                                ]
                            ],
                        },
                    },
                    {
                        "type": "Feature",
                        "properties": {
                            "name": "Koreatown",
                            "city": "Los Angeles city, California",
                            "state": "ca",
                        },
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-118.31, 34.05],
                                    [-118.27, 34.05],
                                    [-118.27, 34.08],
                                    [-118.31, 34.08],
                                    [-118.31, 34.05],
                                ]
                            ],
                        },
                    },
                ],
            }
        )
    )

    lookup = NeighborhoodLookup.from_data_dir(tmp_path)
    match = lookup.match(
        34.061, -118.295, city_name="Los Angeles city, California", state_abbrev="ca"
    )

    assert match is not None
    assert match["name"] == "Koreatown"

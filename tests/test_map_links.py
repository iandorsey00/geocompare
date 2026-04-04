import random
from types import SimpleNamespace

import pytest

from geocompare.tools.map_links import (
    google_maps_url,
    pick_street_view_point,
    profile_map_links,
    random_google_street_view_url,
)


def test_google_maps_url_uses_coordinates():
    url = google_maps_url(37.7749, -122.4194)

    assert "https://www.google.com/maps/search/" in url
    assert "query=37.774900%2C-122.419400" in url


def test_random_google_street_view_url_is_deterministic_with_rng():
    url = random_google_street_view_url(37.7749, -122.4194, rng=random.Random(7))

    assert "https://www.google.com/maps/@" in url
    assert "map_action=pano" in url
    assert "viewpoint=37.774900%2C-122.419400" in url
    assert "heading=" in url


def test_profile_map_links_requires_coordinates():
    profile = SimpleNamespace(rc={})

    with pytest.raises(ValueError):
        profile_map_links(profile)


def test_pick_street_view_point_prefers_osm_road_points_inside_boundary():
    profile = SimpleNamespace(
        rc={"latitude": 37.0, "longitude": -122.0},
        boundary=[(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
    )

    def requester(_query, _url, _timeout):
        return {
            "elements": [
                {"tags": {"highway": "primary"}, "center": {"lat": 1.5, "lon": 1.5}},
                {"tags": {"highway": "primary"}, "center": {"lat": 3.0, "lon": 3.0}},
            ]
        }

    point, source = pick_street_view_point(profile, rng=random.Random(1), requester=requester)

    assert point == (1.5, 1.5)
    assert source == "road"


def test_pick_street_view_point_can_bias_to_arterials():
    profile = SimpleNamespace(
        rc={"latitude": 37.0, "longitude": -122.0},
        boundary=[(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
    )

    def requester(_query, _url, _timeout):
        return {
            "elements": [
                {"tags": {"highway": "residential"}, "center": {"lat": 1.0, "lon": 1.0}},
                {"tags": {"highway": "primary"}, "center": {"lat": 1.5, "lon": 1.5}},
            ]
        }

    point, source = pick_street_view_point(
        profile,
        rng=random.Random(1),
        requester=requester,
        street_bias="arterials",
    )

    assert point == (1.5, 1.5)
    assert source == "road"


def test_pick_street_view_point_can_bias_to_local_streets():
    profile = SimpleNamespace(
        rc={"latitude": 37.0, "longitude": -122.0},
        boundary=[(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
    )

    def requester(_query, _url, _timeout):
        return {
            "elements": [
                {"tags": {"highway": "primary"}, "center": {"lat": 1.5, "lon": 1.5}},
                {"tags": {"highway": "tertiary"}, "center": {"lat": 1.0, "lon": 1.0}},
            ]
        }

    point, source = pick_street_view_point(
        profile,
        rng=random.Random(1),
        requester=requester,
        street_bias="local-streets",
    )

    assert point == (1.0, 1.0)
    assert source == "road"


def test_pick_street_view_point_falls_back_to_random_boundary_point_when_road_lookup_fails():
    profile = SimpleNamespace(
        rc={"latitude": 37.0, "longitude": -122.0},
        boundary=[(0.0, 0.0), (0.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
    )

    point, source = pick_street_view_point(
        profile,
        rng=random.Random(7),
        requester=lambda *_args, **_kwargs: {"elements": []},
    )

    assert 0.0 <= point[0] <= 2.0
    assert 0.0 <= point[1] <= 2.0
    assert source == "boundary"


def test_pick_street_view_point_falls_back_to_centroid_when_no_boundary_point_can_be_produced():
    profile = SimpleNamespace(
        rc={"latitude": 37.0, "longitude": -122.0},
        boundary=[(1.0, 1.0), (1.0, 1.0), (1.0, 1.0)],
    )

    point, source = pick_street_view_point(
        profile,
        rng=random.Random(3),
        requester=lambda *_args, **_kwargs: {"elements": []},
    )

    assert point == (37.0, -122.0)
    assert source == "centroid"

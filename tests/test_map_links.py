import random
from types import SimpleNamespace

import pytest

from geocompare.tools.map_links import (
    google_maps_url,
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

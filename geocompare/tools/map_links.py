import random
from urllib.parse import urlencode


def _format_lat_lon(latitude, longitude):
    return f"{float(latitude):.6f},{float(longitude):.6f}"


def google_maps_url(latitude, longitude):
    query = urlencode({"api": 1, "query": _format_lat_lon(latitude, longitude)})
    return f"https://www.google.com/maps/search/?{query}"


def random_google_street_view_url(latitude, longitude, rng=None):
    generator = rng if rng is not None else random.Random()
    query = urlencode(
        {
            "api": 1,
            "map_action": "pano",
            "viewpoint": _format_lat_lon(latitude, longitude),
            "heading": f"{generator.uniform(0.0, 360.0):.1f}",
            "pitch": "0",
            "fov": "80",
        }
    )
    return f"https://www.google.com/maps/@?{query}"


def profile_map_links(profile, rng=None):
    latitude = getattr(profile, "rc", {}).get("latitude")
    longitude = getattr(profile, "rc", {}).get("longitude")
    if latitude is None or longitude is None:
        raise ValueError(
            "Sorry, map links are unavailable because this geography lacks coordinates."
        )

    return {
        "google_maps_url": google_maps_url(latitude, longitude),
        "google_street_view_url": random_google_street_view_url(
            latitude,
            longitude,
            rng=rng,
        ),
    }

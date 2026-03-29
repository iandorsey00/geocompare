import json
import random
import urllib.error
import urllib.parse
import urllib.request


def _format_lat_lon(latitude, longitude):
    return f"{float(latitude):.6f},{float(longitude):.6f}"


def _normalize_ring(ring):
    points = []
    for point in ring:
        if isinstance(point, dict):
            latitude = point.get("lat")
            longitude = point.get("lon")
        else:
            latitude, longitude = point
        points.append((float(latitude), float(longitude)))
    if len(points) >= 2 and points[0] != points[-1]:
        points.append(points[0])
    return points


def _extract_boundary_polygons(profile):
    candidates = [
        getattr(profile, "boundary", None),
        getattr(profile, "geometry", None),
        getattr(profile, "rc", {}).get("boundary"),
        getattr(profile, "rc", {}).get("geometry"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        if isinstance(candidate, dict):
            geometry_type = candidate.get("type")
            coordinates = candidate.get("coordinates", [])
            if geometry_type == "Polygon":
                return [_normalize_ring(coordinates[0])] if coordinates else []
            if geometry_type == "MultiPolygon":
                return [_normalize_ring(polygon[0]) for polygon in coordinates if polygon]
        if isinstance(candidate, list) and candidate:
            first = candidate[0]
            if isinstance(first, tuple) and len(first) == 2:
                return [_normalize_ring(candidate)]
            if isinstance(first, list) and first and not isinstance(first[0], (list, tuple, dict)):
                return [_normalize_ring(candidate)]
            if isinstance(first, list):
                return [_normalize_ring(ring) for ring in candidate if ring]
    return []


def _point_in_ring(latitude, longitude, ring):
    inside = False
    for idx in range(len(ring) - 1):
        lat1, lon1 = ring[idx]
        lat2, lon2 = ring[idx + 1]
        intersects = ((lon1 > longitude) != (lon2 > longitude)) and (
            latitude < (lat2 - lat1) * (longitude - lon1) / ((lon2 - lon1) or 1e-12) + lat1
        )
        if intersects:
            inside = not inside
    return inside


def _point_in_polygons(latitude, longitude, polygons):
    return any(_point_in_ring(latitude, longitude, polygon) for polygon in polygons)


def _boundary_bbox(polygons):
    latitudes = [latitude for polygon in polygons for latitude, _ in polygon]
    longitudes = [longitude for polygon in polygons for _, longitude in polygon]
    return min(latitudes), min(longitudes), max(latitudes), max(longitudes)


def _random_point_in_polygons(polygons, rng=None, max_attempts=250):
    if not polygons:
        return None
    min_lat, min_lon, max_lat, max_lon = _boundary_bbox(polygons)
    if min_lat == max_lat or min_lon == max_lon:
        return None

    generator = rng if rng is not None else random.Random()
    for _ in range(max_attempts):
        latitude = generator.uniform(min_lat, max_lat)
        longitude = generator.uniform(min_lon, max_lon)
        if _point_in_polygons(latitude, longitude, polygons):
            return latitude, longitude
    return None


def _default_overpass_request(query, overpass_url, timeout):
    request = urllib.request.Request(
        overpass_url,
        data=query.encode("utf-8"),
        headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def _osm_road_points_within_boundary(polygons, requester=None, timeout=12):
    if not polygons:
        return []

    south, west, north, east = _boundary_bbox(polygons)
    if south == north or west == east:
        return []

    query = (
        "[out:json][timeout:12];"
        f'way["highway"]({south:.6f},{west:.6f},{north:.6f},{east:.6f});'
        "out center;"
    )
    request_fn = requester or _default_overpass_request
    try:
        payload = request_fn(query, "https://overpass-api.de/api/interpreter", timeout)
        if isinstance(payload, bytes):
            payload = json.loads(payload.decode("utf-8"))
    except (OSError, ValueError, urllib.error.URLError):
        return []

    elements = payload.get("elements", []) if isinstance(payload, dict) else []
    points = []
    for element in elements:
        center = element.get("center") or {}
        latitude = center.get("lat")
        longitude = center.get("lon")
        if latitude is None or longitude is None:
            continue
        latitude = float(latitude)
        longitude = float(longitude)
        if _point_in_polygons(latitude, longitude, polygons):
            points.append((latitude, longitude))
    return points


def google_maps_url(latitude, longitude):
    query = urllib.parse.urlencode({"api": 1, "query": _format_lat_lon(latitude, longitude)})
    return f"https://www.google.com/maps/search/?{query}"


def random_google_street_view_url(latitude, longitude, rng=None):
    generator = rng if rng is not None else random.Random()
    query = urllib.parse.urlencode(
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


def pick_street_view_point(profile, rng=None, requester=None):
    latitude = getattr(profile, "rc", {}).get("latitude")
    longitude = getattr(profile, "rc", {}).get("longitude")
    if latitude is None or longitude is None:
        raise ValueError(
            "Sorry, map links are unavailable because this geography lacks coordinates."
        )

    polygons = _extract_boundary_polygons(profile)
    generator = rng if rng is not None else random.Random()
    if polygons:
        road_points = _osm_road_points_within_boundary(polygons, requester=requester)
        if road_points:
            return generator.choice(road_points), "road"

        boundary_point = _random_point_in_polygons(polygons, rng=generator)
        if boundary_point is not None:
            return boundary_point, "boundary"

    return (float(latitude), float(longitude)), "centroid"


def profile_map_links(profile, rng=None, requester=None):
    latitude = getattr(profile, "rc", {}).get("latitude")
    longitude = getattr(profile, "rc", {}).get("longitude")
    if latitude is None or longitude is None:
        raise ValueError(
            "Sorry, map links are unavailable because this geography lacks coordinates."
        )

    street_view_point, point_source = pick_street_view_point(
        profile,
        rng=rng,
        requester=requester,
    )

    return {
        "google_maps_url": google_maps_url(latitude, longitude),
        "google_street_view_url": random_google_street_view_url(
            street_view_point[0],
            street_view_point[1],
            rng=rng,
        ),
        "google_street_view_point_source": point_source,
    }

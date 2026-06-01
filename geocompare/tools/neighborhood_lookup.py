import json
from pathlib import Path

from geocompare.tools.geography_names import compact_place_name


def _point_in_ring(latitude, longitude, ring):
    inside = False
    point_x = float(longitude)
    point_y = float(latitude)

    if len(ring) < 3:
        return False

    previous_x, previous_y = ring[-1]
    for current_x, current_y in ring:
        x1 = float(previous_x)
        y1 = float(previous_y)
        x2 = float(current_x)
        y2 = float(current_y)

        intersects = ((y1 > point_y) != (y2 > point_y)) and (
            point_x < (x2 - x1) * (point_y - y1) / ((y2 - y1) or 1e-12) + x1
        )
        if intersects:
            inside = not inside
        previous_x, previous_y = current_x, current_y

    return inside


def _point_in_polygon(latitude, longitude, polygon):
    if not polygon:
        return False
    if not _point_in_ring(latitude, longitude, polygon[0]):
        return False
    for hole in polygon[1:]:
        if _point_in_ring(latitude, longitude, hole):
            return False
    return True


class NeighborhoodLookup:
    """Optional centroid-to-neighborhood lookup from local GeoJSON polygons."""

    def __init__(self, neighborhoods=None):
        self.neighborhoods = list(neighborhoods or [])

    @classmethod
    def from_data_dir(cls, data_dir):
        data_dir = Path(data_dir)
        bundled = Path(__file__).resolve().parents[1] / "reference" / "neighborhoods_seed.geojson"
        candidates = [
            bundled,
            data_dir / "reference" / "neighborhoods.geojson",
            data_dir / "reference" / "neighborhoods.json",
        ]
        neighborhoods = []
        for path in candidates:
            if path.exists():
                neighborhoods.extend(cls._load_geojson(path))
        return cls(neighborhoods)

    @staticmethod
    def _load_geojson(path):
        with open(path, "rt") as f:
            payload = json.load(f)

        if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
            raise ValueError("Neighborhood reference must be a GeoJSON FeatureCollection.")

        neighborhoods = []
        for feature in payload.get("features", []):
            geometry = feature.get("geometry") or {}
            properties = feature.get("properties") or {}
            polygons = NeighborhoodLookup._extract_polygons(geometry)
            if not polygons:
                continue

            name = str(properties.get("name") or "").strip()
            if not name:
                continue

            city = str(properties.get("city") or "").strip()
            state = str(properties.get("state") or "").strip().lower()
            bbox = NeighborhoodLookup._bbox_for_polygons(polygons)
            bbox_area = (bbox[2] - bbox[0]) * (bbox[3] - bbox[1])
            neighborhoods.append(
                {
                    "name": name,
                    "city": city,
                    "city_compact": compact_place_name(city),
                    "state": state,
                    "polygons": polygons,
                    "bbox": bbox,
                    "bbox_area": bbox_area,
                }
            )
        return neighborhoods

    @staticmethod
    def _extract_polygons(geometry):
        geometry_type = geometry.get("type")
        coordinates = geometry.get("coordinates") or []
        if geometry_type == "Polygon":
            return [coordinates]
        if geometry_type == "MultiPolygon":
            return list(coordinates)
        return []

    @staticmethod
    def _bbox_for_polygons(polygons):
        xs = []
        ys = []
        for polygon in polygons:
            for ring in polygon:
                for longitude, latitude in ring:
                    xs.append(float(longitude))
                    ys.append(float(latitude))
        return (min(xs), min(ys), max(xs), max(ys))

    def match(self, latitude, longitude, city_name=None, state_abbrev=None):
        if latitude is None or longitude is None or not self.neighborhoods:
            return None

        point_lat = float(latitude)
        point_lon = float(longitude)
        compact_city = compact_place_name(city_name)
        state_code = str(state_abbrev or "").strip().lower()
        matches = []

        for neighborhood in self.neighborhoods:
            if state_code and neighborhood["state"] and neighborhood["state"] != state_code:
                continue

            min_x, min_y, max_x, max_y = neighborhood["bbox"]
            if point_lon < min_x or point_lon > max_x or point_lat < min_y or point_lat > max_y:
                continue

            if not any(
                _point_in_polygon(point_lat, point_lon, polygon)
                for polygon in neighborhood["polygons"]
            ):
                continue

            city_match = bool(
                compact_city
                and neighborhood["city_compact"]
                and neighborhood["city_compact"].lower() == compact_city.lower()
            )
            matches.append((1 if city_match else 0, -neighborhood["bbox_area"], neighborhood))

        if not matches:
            return None

        matches.sort(reverse=True)
        return matches[0][2]

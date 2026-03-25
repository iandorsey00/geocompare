import importlib.util
import os
from functools import lru_cache
from pathlib import Path

from geocompare.repository.sqlite_repository import SQLiteRepository
from geocompare.services.query_service import QueryService


def _optional_dependency_error(package_name):
    raise RuntimeError(
        f"Missing optional dependency: {package_name}. "
        f'Install the web extras with: python3 -m pip install -e ".[web]"'
    )


def _format_display_name(profile, official_labels=False):
    if official_labels and getattr(profile, "sumlevel", None) == "140":
        return getattr(profile, "canonical_name", profile.name)
    return profile.name


def _serialize_profile(profile, official_labels=False, include_metrics=True):
    payload = {
        "name": _format_display_name(profile, official_labels=official_labels),
        "display_name": profile.name,
        "canonical_name": getattr(profile, "canonical_name", profile.name),
        "sumlevel": getattr(profile, "sumlevel", None),
        "state": getattr(profile, "state", None),
        "geoid": getattr(profile, "geoid", None),
        "population": getattr(profile, "rc", {}).get("population") if getattr(profile, "rc", None) else None,
        "counties": list(getattr(profile, "counties", []) or []),
        "counties_display": list(getattr(profile, "counties_display", []) or []),
    }
    if include_metrics:
        payload["metrics"] = dict(getattr(profile, "fc", {}) or {})
    return payload


def _resolve_metric_display(service, data_identifier, sample_profile):
    resolved = service.resolve_data_identifier(data_identifier, sample_profile)
    return resolved["display_store"], resolved["key"], resolved["label"]


def _serialize_remoteness_row(service, row, data_identifier, official_labels=False, kilometers=False):
    candidate = row["candidate"]
    nearest = row["nearest_match"]
    display_store, key, label = _resolve_metric_display(service, data_identifier, candidate)
    distance_miles = float(row["distance_miles"])
    distance = distance_miles * 1.609344 if kilometers else distance_miles
    return {
        "candidate": _serialize_profile(candidate, official_labels=official_labels, include_metrics=False),
        "nearest_match": _serialize_profile(nearest, official_labels=official_labels, include_metrics=False),
        "metric_label": label,
        "candidate_value": getattr(candidate, display_store)[key],
        "nearest_match_value": getattr(nearest, display_store)[key],
        "distance_miles": distance_miles,
        "distance": distance,
        "distance_unit": "km" if kilometers else "mi",
    }


def _serialize_local_average_row(
    service,
    row,
    data_identifier,
    official_labels=False,
    kilometers=False,
):
    candidate = row["candidate"]
    display_store, key, label = _resolve_metric_display(service, data_identifier, candidate)
    span_miles = float(row["neighbor_span_miles"])
    span = span_miles * 1.609344 if kilometers else span_miles
    return {
        "candidate": _serialize_profile(candidate, official_labels=official_labels, include_metrics=False),
        "metric_label": label,
        "candidate_value": getattr(candidate, display_store)[key],
        "local_average": float(row["local_average"]),
        "neighbor_span_miles": span_miles,
        "neighbor_span": span,
        "span_unit": "km" if kilometers else "mi",
    }


def _serialize_ranking_row(service, profile, data_identifier, official_labels=False):
    display_store, key, label = _resolve_metric_display(service, data_identifier, profile)
    return {
        "geography": _serialize_profile(profile, official_labels=official_labels, include_metrics=False),
        "metric_label": label,
        "metric_value": getattr(profile, display_store)[key],
    }


def _serialize_nearest_row(profile, distance_miles, official_labels=False, kilometers=False):
    distance = float(distance_miles) * 1.609344 if kilometers else float(distance_miles)
    return {
        "geography": _serialize_profile(profile, official_labels=official_labels, include_metrics=False),
        "distance_miles": float(distance_miles),
        "distance": distance,
        "distance_unit": "km" if kilometers else "mi",
    }


def _build_service(sqlite_path=None):
    service = QueryService()
    target_path = sqlite_path or os.getenv("GEOCOMPARE_SQLITE_PATH")
    if target_path:
        sqlite_file = Path(target_path).expanduser().resolve()
        service.sqlite_path = sqlite_file
        service.sqlite_repository = SQLiteRepository(sqlite_file)
        service.primary_repository = service.sqlite_repository
        service.d = None
        service._dp_by_name = {}
        service._gv_by_name = {}
        service._data_identifier_index = {}
        service.identity_index = None
        service._us_dp_cache = None
    return service


@lru_cache(maxsize=1)
def get_service():
    return _build_service()


def create_app():
    if importlib.util.find_spec("fastapi") is None:
        _optional_dependency_error("fastapi")
    from fastapi import FastAPI, HTTPException, Query

    app = FastAPI(title="GeoCompare API", version="0.8.0")

    @app.get("/health")
    def health():
        service = get_service()
        return {
            "status": "ok",
            "repository": str(service.primary_repository.name),
        }

    @app.get("/search")
    def search(q: str = Query(..., min_length=1), n: int = Query(10, ge=1, le=100)):
        service = get_service()
        results = service.display_label_search(query=q, n=n)
        return {
            "query": q,
            "count": len(results),
            "results": [
                _serialize_profile(profile, include_metrics=False)
                for profile in results
            ],
        }

    @app.get("/profile")
    def profile(name: str | None = None, geoid: str | None = None, official_labels: bool = False):
        service = get_service()
        if not name and not geoid:
            raise HTTPException(status_code=400, detail="Provide either name or geoid.")
        try:
            if geoid:
                profile_obj = service._fetch_profile_by_geoid(geoid)
            else:
                profile_obj = service._fetch_profile_by_name(name)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return _serialize_profile(profile_obj, official_labels=official_labels, include_metrics=True)

    @app.get("/sources")
    def sources():
        service = get_service()
        rows = service.sources()
        return {
            "count": len(rows),
            "results": rows,
        }

    @app.get("/resolve")
    def resolve(
        query: str,
        state: str | None = None,
        sumlevel: str | None = None,
        population: int | None = None,
        n: int = Query(5, ge=1, le=25),
    ):
        service = get_service()
        results = service.resolve_geography(
            query=query,
            state=state,
            sumlevel=sumlevel,
            population=population,
            n=n,
        )
        return {
            "query": query,
            "count": len(results),
            "results": results,
        }

    @app.get("/remoteness")
    def remoteness(
        data_identifier: str,
        threshold: str,
        target: str = "below",
        scope: str = "tracts+",
        where: str = "",
        match_where: str = "",
        n: int = Query(15, ge=1, le=100),
        county_population_min: int | None = None,
        county_density_min: float | None = None,
        one_per_county: bool = False,
        official_labels: bool = False,
        kilometers: bool = False,
    ):
        service = get_service()
        try:
            rows = service.remoteness(
                data_identifier=data_identifier,
                threshold=threshold,
                target=target,
                context=scope,
                geofilter=where,
                match_geofilter=match_where,
                n=n,
                county_population_min=county_population_min,
                county_density_min=county_density_min,
                one_per_county=one_per_county,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        return {
            "data_identifier": data_identifier,
            "threshold": threshold,
            "target": target,
            "scope": scope,
            "where": where,
            "match_where": match_where,
            "count": len(rows),
            "results": [
                _serialize_remoteness_row(
                    service,
                    row,
                    data_identifier=data_identifier,
                    official_labels=official_labels,
                    kilometers=kilometers,
                )
                for row in rows
            ],
        }

    @app.get("/local-average")
    def local_average(
        data_identifier: str,
        scope: str = "tracts+",
        where: str = "",
        n: int = Query(15, ge=1, le=100),
        neighbors: int = Query(20, ge=1, le=250),
        county_population_min: int | None = None,
        county_density_min: float | None = None,
        one_per_county: bool = False,
        official_labels: bool = False,
        kilometers: bool = False,
    ):
        service = get_service()
        try:
            rows = service.local_average(
                data_identifier=data_identifier,
                context=scope,
                geofilter=where,
                n=n,
                neighbors=neighbors,
                county_population_min=county_population_min,
                county_density_min=county_density_min,
                one_per_county=one_per_county,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        return {
            "data_identifier": data_identifier,
            "scope": scope,
            "neighbors": neighbors,
            "count": len(rows),
            "results": [
                _serialize_local_average_row(
                    service,
                    row,
                    data_identifier=data_identifier,
                    official_labels=official_labels,
                    kilometers=kilometers,
                )
                for row in rows
            ],
        }

    @app.get("/top")
    def top(
        data_identifier: str,
        scope: str = "tracts+",
        where: str = "",
        n: int = Query(15, ge=1, le=100),
        official_labels: bool = False,
    ):
        service = get_service()
        try:
            rows = service.extreme_values(
                data_identifier=data_identifier,
                context=scope,
                geofilter=where,
                n=n,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        metric_label = _resolve_metric_display(service, data_identifier, rows[0])[2] if rows else data_identifier

        return {
            "data_identifier": data_identifier,
            "metric_label": metric_label,
            "scope": scope,
            "count": len(rows),
            "results": [
                _serialize_ranking_row(
                    service,
                    row,
                    data_identifier=data_identifier,
                    official_labels=official_labels,
                )
                for row in rows
            ],
        }

    @app.get("/bottom")
    def bottom(
        data_identifier: str,
        scope: str = "tracts+",
        where: str = "",
        n: int = Query(15, ge=1, le=100),
        official_labels: bool = False,
    ):
        service = get_service()
        try:
            rows = service.lowest_values(
                data_identifier=data_identifier,
                context=scope,
                geofilter=where,
                n=n,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        metric_label = _resolve_metric_display(service, data_identifier, rows[0])[2] if rows else data_identifier

        return {
            "data_identifier": data_identifier,
            "metric_label": metric_label,
            "scope": scope,
            "count": len(rows),
            "results": [
                _serialize_ranking_row(
                    service,
                    row,
                    data_identifier=data_identifier,
                    official_labels=official_labels,
                )
                for row in rows
            ],
        }

    @app.get("/nearest")
    def nearest(
        name: str,
        scope: str = "places+",
        where: str = "",
        n: int = Query(10, ge=1, le=100),
        official_labels: bool = False,
        kilometers: bool = False,
    ):
        service = get_service()
        try:
            profile_obj = service.get_dp(display_label=name)[0]
            rows = service.closest_geographies(
                display_label=profile_obj.name,
                context=scope,
                geofilter=where,
                n=n,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        return {
            "query": name,
            "scope": scope,
            "count": len(rows),
            "results": [
                _serialize_nearest_row(
                    profile,
                    distance_miles,
                    official_labels=official_labels,
                    kilometers=kilometers,
                )
                for profile, distance_miles in rows
            ],
        }

    return app


def main():
    if importlib.util.find_spec("uvicorn") is None:
        _optional_dependency_error("uvicorn")
    import uvicorn

    host = os.getenv("GEOCOMPARE_API_HOST", "127.0.0.1")
    port = int(os.getenv("GEOCOMPARE_API_PORT", "8000"))
    uvicorn.run(create_app(), host=host, port=port)

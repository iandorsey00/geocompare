from types import SimpleNamespace

import pytest

from geocompare.interfaces import api as api_module


class _DummyGeoVector:
    def __init__(
        self, name, canonical_name=None, sumlevel="160", state="ca", geoid="16000US000000"
    ):
        self.name = name
        self.canonical_name = canonical_name or name
        self.sumlevel = sumlevel
        self.state = state
        self.geoid = geoid
        self.counties = ["06059"]
        self.counties_display = ["Orange County"]
        self.rc = {"population": 1000}
        self.fc = {}
        self._distances = {}

    def distance(self, other, mode="std"):
        return self._distances[(other.name, mode)]


def _build_similarity_rows(mode="std"):
    target = _DummyGeoVector(
        "Mission Viejo city, California", canonical_name="Mission Viejo city, California"
    )
    peer = _DummyGeoVector("Huntington Beach city, California")
    tract = _DummyGeoVector(
        "626.37, near Aliso Viejo, Orange County, CA",
        canonical_name="Census Tract 626.37, Orange County, California",
        sumlevel="140",
        geoid="14000US06059062637",
    )
    target._distances[(target.name, mode)] = 0.0
    target._distances[(peer.name, mode)] = 3.05
    target._distances[(tract.name, mode)] = 2.8
    return [target, peer, tract]


def _client(monkeypatch, compare_impl):
    TestClient = pytest.importorskip("fastapi.testclient").TestClient
    service = SimpleNamespace(compare_geovectors=compare_impl)
    monkeypatch.setattr(api_module, "get_service", lambda: service)
    return TestClient(api_module.create_app())


def test_similar_peer_default(monkeypatch):
    captured = {}

    def compare_geovectors(**kwargs):
        captured.update(kwargs)
        return _build_similarity_rows("std")[:2]

    client = _client(monkeypatch, compare_geovectors)
    response = client.get("/similar", params={"name": "Mission Viejo city, California", "n": 2})

    assert response.status_code == 200
    body = response.json()
    assert body["query"] == "Mission Viejo city, California"
    assert body["mode"] == "similar"
    assert body["count"] == 2
    assert body["results"][0]["geography"]["name"] == "Mission Viejo city, California"
    assert body["results"][0]["distance"] == 0.0
    assert captured["display_label"] == "Mission Viejo city, California"
    assert captured["context"] == ""
    assert captured["universes"] is None
    assert captured["mode"] == "std"


def test_similar_accepts_single_universe(monkeypatch):
    captured = {}

    def compare_geovectors(**kwargs):
        captured.update(kwargs)
        return _build_similarity_rows("std")[:2]

    client = _client(monkeypatch, compare_geovectors)
    response = client.get(
        "/similar",
        params={"name": "Mission Viejo city, California", "universe": "places", "n": 2},
    )

    assert response.status_code == 200
    assert captured["context"] == "places+"
    assert captured["universes"] is None


def test_similar_accepts_multiple_universes(monkeypatch):
    captured = {}

    def compare_geovectors(**kwargs):
        captured.update(kwargs)
        return _build_similarity_rows("std")

    client = _client(monkeypatch, compare_geovectors)
    response = client.get(
        "/similar",
        params={"name": "Mission Viejo city, California", "universes": "places,tracts", "n": 3},
    )

    assert response.status_code == 200
    assert captured["context"] == ""
    assert captured["universes"] == "places,tracts"
    assert response.json()["results"][2]["geography"]["sumlevel"] == "140"


def test_similar_form_uses_built_form_mode(monkeypatch):
    captured = {}

    def compare_geovectors(**kwargs):
        captured.update(kwargs)
        return _build_similarity_rows("app")[:2]

    client = _client(monkeypatch, compare_geovectors)
    response = client.get(
        "/similar-form", params={"name": "Mission Viejo city, California", "n": 2}
    )

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "similar-form"
    assert captured["mode"] == "app"


def test_similar_rejects_universe_and_universes_together(monkeypatch):
    client = _client(monkeypatch, lambda **kwargs: _build_similarity_rows("std"))
    response = client.get(
        "/similar",
        params={
            "name": "Mission Viejo city, California",
            "universe": "places",
            "universes": "places,tracts",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Use either universe or universes, not both."


def test_similar_honors_official_labels_for_tracts(monkeypatch):
    client = _client(monkeypatch, lambda **kwargs: _build_similarity_rows("std"))
    response = client.get(
        "/similar",
        params={
            "name": "Mission Viejo city, California",
            "universes": "places,tracts",
            "official_labels": "true",
        },
    )

    assert response.status_code == 200
    tract_row = response.json()["results"][2]
    assert tract_row["geography"]["name"] == "Census Tract 626.37, Orange County, California"

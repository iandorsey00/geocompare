from types import SimpleNamespace

from geocompare.identity.place_identity import PlaceIdentityIndex


def _dp(name, state, sumlevel, geoid, population):
    return SimpleNamespace(
        name=name,
        state=state,
        sumlevel=sumlevel,
        geoid=geoid,
        rc={"population": population},
    )


def test_normalize_name_removes_suffix_and_state():
    assert PlaceIdentityIndex.normalize_name("San Francisco city, California") == "san francisco"


def test_resolve_prefers_state_match():
    idx = PlaceIdentityIndex.from_demographic_profiles(
        [
            _dp("Springfield city, Illinois", "il", "160", "16000US1772000", 113000),
            _dp("Springfield city, Missouri", "mo", "160", "16000US2970000", 169000),
        ]
    )

    result = idx.resolve("Springfield", state="mo", limit=1)
    assert result[0]["state"] == "mo"


def test_resolve_supports_tract_geoid_alias():
    tract = _dp(
        "601, near San Francisco, San Francisco County, CA",
        "ca",
        "140",
        "1400000US06075060100",
        4500,
    )
    tract.canonical_name = "Census Tract 601, San Francisco County, California"
    idx = PlaceIdentityIndex.from_demographic_profiles([tract])

    result = idx.resolve("06075060100", sumlevel="140", limit=1)
    assert result[0]["geoid"] == "1400000US06075060100"


def test_resolve_supports_formal_tract_name_alias():
    tract = _dp(
        "9601, near Pahrump, Nye County, NV",
        "nv",
        "140",
        "1400000US32023960100",
        3000,
    )
    tract.canonical_name = "Census Tract 9601, Nye County, Nevada"
    idx = PlaceIdentityIndex.from_demographic_profiles([tract])

    result = idx.resolve("Census Tract 9601, Nye County, Nevada", sumlevel="140", limit=1)
    assert result[0]["name"] == "9601, near Pahrump, Nye County, NV"

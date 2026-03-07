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

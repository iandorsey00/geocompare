from types import SimpleNamespace

from geocompare.interfaces.api import (
    _serialize_local_average_row,
    _serialize_profile,
    _serialize_remoteness_row,
)


def _profile(name, official_name, sumlevel="140"):
    return SimpleNamespace(
        name=name,
        canonical_name=official_name,
        sumlevel=sumlevel,
        state="ca",
        geoid="14000US000000",
        counties=["06001"],
        counties_display=["Alameda County"],
        fc={"population": "5,000", "median_household_income": "$100,000"},
    )


def test_serialize_profile_honors_official_labels_for_tracts():
    profile = _profile("4515.05, near Livermore, Alameda County, CA", "Census Tract 4515.05, Alameda County, California")

    humanized = _serialize_profile(profile, official_labels=False, include_metrics=False)
    official = _serialize_profile(profile, official_labels=True, include_metrics=False)

    assert humanized["name"] == "4515.05, near Livermore, Alameda County, CA"
    assert official["name"] == "Census Tract 4515.05, Alameda County, California"


def test_serialize_remoteness_row_converts_distance_units():
    candidate = _profile("Candidate", "Official Candidate")
    nearest = _profile("Nearest", "Official Nearest")
    service = SimpleNamespace(
        resolve_data_identifier=lambda data_identifier, sample: {
            "display_store": "fc",
            "key": data_identifier,
            "label": "Median household income",
        }
    )

    row = {"candidate": candidate, "nearest_match": nearest, "distance_miles": 10.0}
    payload = _serialize_remoteness_row(
        service,
        row,
        data_identifier="median_household_income",
        kilometers=True,
    )

    assert payload["distance_miles"] == 10.0
    assert round(payload["distance"], 3) == 16.093
    assert payload["distance_unit"] == "km"


def test_serialize_local_average_row_converts_span_units():
    candidate = _profile("Candidate", "Official Candidate")
    service = SimpleNamespace(
        resolve_data_identifier=lambda data_identifier, sample: {
            "display_store": "fc",
            "key": data_identifier,
            "label": "Median household income",
        }
    )

    row = {"candidate": candidate, "local_average": 123456.0, "neighbor_span_miles": 5.0}
    payload = _serialize_local_average_row(
        service,
        row,
        data_identifier="median_household_income",
        kilometers=False,
    )

    assert payload["local_average"] == 123456.0
    assert payload["neighbor_span"] == 5.0
    assert payload["span_unit"] == "mi"

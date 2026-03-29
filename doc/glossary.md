# Glossary

## Category

A shorthand group of related data identifiers used by `geocompare export rows`.

## Data identifier

The canonical CLI/query name for a metric, such as `population` or
`median_household_income`.

## DemographicProfile

The primary model used to represent one geography and its profile metrics.

## Display label

The exact geography name string shown to users, for example
`Seattle city, Washington`.

## Gazetteer files

Files that provide geographic reference data such as land area and coordinates.

## GEOID

A geographic identifier used by Census datasets. Short GEOIDs are not globally
unique across all summary levels.

## GeoVector

A derived vector used for similarity comparisons between geographies. GeoCompare
currently includes a standard demographic GeoVector and a built-form GeoVector.

## LOGRECNO

The logical record number used to join Census data files to geography files.
It is unique only within a state context.

## Scope

The query constraint formed by a universe plus an optional grouping geography.

## Summary level

A geography type such as nation, state, county, place, CBSA, urban area, or ZCTA.

Supported summary level codes include:

| Code | Description |
|---|---|
| `010` | Nation |
| `040` | State |
| `050` | County |
| `160` | Place |
| `310` | CBSA |
| `400` | Urban area |
| `860` | ZCTA |

## Universe

The type of geography being queried within a scope, such as `places` or `counties`.

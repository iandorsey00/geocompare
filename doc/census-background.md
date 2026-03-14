# About the U.S. Census Bureau

GeoCompare uses U.S. Census Bureau data sources. This background explains why
summary levels, GEOIDs, and ACS source files matter to the build pipeline.

## Geographies

The Census Bureau differentiates geography types using summary levels, such as:

- nation
- state
- county
- place
- ZCTA

There are many other summary levels in Census products. The naming scheme and
code list are documented by the Census Bureau:

- [Cartographic boundary summary levels](https://www.census.gov/programs-surveys/geography/technical-documentation/naming-convention/cartographic-boundary-file/carto-boundary-summary-level.html)
- [Geographic identifiers guidance](https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html)

Places can cross county lines, which makes grouping and containment questions
less trivial than they first appear.

## What are ZCTAs?

ZCTAs are Census approximations of ZIP codes. ZIP codes are USPS delivery
routes, not formal geography polygons, so the Census had to approximate them as
areas for tabulation purposes.

Useful references:

- [Wikipedia: ZIP Code](https://en.wikipedia.org/wiki/ZIP_Code)
- [Archived Missouri Census Data Center note on ZIP resources](https://web.archive.org/web/20050112111712/http://mcdc2.missouri.edu/webrepts/geography/ZIP.resources.html)

Important implications:

- ZIP city names do not guarantee municipal boundaries.
- ZCTAs can be awkward to group cleanly.
- Short GEOID collisions can happen across geography types, which is why
  GeoCompare uses geo-level-aware overlay matching when level hints are present.

## Summary files

ACS summary files consist of geography files and data files.

- Geography files include names, GEOIDs, and `LOGRECNO`.
- Data files include table values keyed by `LOGRECNO`.

Because the ACS is large, data is split across files by state and, in older
layouts, by sequence number. GeoCompare resolves the necessary table and line
number mappings during build.

## Current build model

GeoCompare currently supports the more recent table-based ACS layout as well as
older sequence-based layouts. It scans the newest compatible files in the input
directory and constructs local query products from them.

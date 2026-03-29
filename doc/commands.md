# Commands

GeoCompare stores runtime data products in SQLite at `bin/default.sqlite`.

## Command structure

```bash
geocompare [--log-level LEVEL] [--version] <command> [subcommand] [options]
```

Top-level commands:

- `build`
- `sources`
- `query`
- `resolve`
- `export`

## Build

```bash
geocompare build /path/to/data
```

Notes:

- `build` auto-detects the latest ACS and gazetteer years in the data directory.
- Overlay files, when present, are attached during build.
- Overlay metric names become queryable data identifiers.

## Sources

```bash
geocompare sources
geocompare sources --format json
```

Notes:

- `sources` shows built-in source information for ACS, Census geography metadata, and the canonical base overlays.
- Personal or custom overlay metrics are intentionally excluded.

## Query

### Search

```bash
geocompare query search "San Jose" -n 10
geocompare query search "san" --format json
```

### Profile

```bash
geocompare query profile "San Francisco city, California"
geocompare query profile "San Francisco city, California" --profile-view compact
geocompare query profile-compare "Mission Viejo city, California" "Carlsbad city, California"
geocompare query map-links "San Francisco city, California"
```

`query map-links` prints:

- an Open in Google Maps URL
- a Random Google Street View URL

Street View targeting now prefers OSM road segment centers inside the geography
boundary when boundary data is available. If that lookup fails, it falls back
to a random boundary point, and then to the centroid. This improves the odds of
Street View success but does not guarantee imagery.

### Similar

```bash
geocompare query similar "Cupertino city, California" --universe places --in-state ny -n 15
geocompare query similar-app "Sunnyvale city, California" --universe counties --in-state nj
```

### Top / Bottom

```bash
geocompare query top per_capita_income --where "population>=50000" --universe places
geocompare query bottom median_year_structure_built --universe places --in-state ny
```

### Nearest

```bash
geocompare query nearest "ZCTA5 94104" --universe places -n 15
```

### Remoteness

```bash
geocompare query remoteness median_household_income 75000 --universe tracts --where "population>=2500"
geocompare query remoteness per_capita_income 35000 --target above --scope "tracts+ca" -n 20
geocompare query remoteness median_household_income 100000 --universe tracts --where "population_density>=2500" --county-population-min 1000000 --one-per-county
geocompare query remoteness per_capita_income 40000 --universe places --where "population>=10000" --match-where "population>=1000"
```

### Local average

```bash
geocompare query local-average median_household_income --universe tracts --neighbors 20 -n 20
geocompare query local-average per_capita_income --scope "tracts+ca" --where "population_density>=2500" --county-population-min 1000000 --one-per-county
```

### Distance

```bash
geocompare query distance "Los Angeles city, California" "San Francisco city, California"
```

## Resolve

Use `resolve` when input is ambiguous and you want canonical results.

```bash
geocompare resolve "San Francisco, CA" --state ca -n 5
geocompare resolve "Springfield" --sumlevel 160 --format json
```

## Export

### Multiple rows

```bash
geocompare export rows "population :income :housing" --universe places --where "population>=100000"
geocompare export rows "population :race" --universe cbsas
```

### Single profile

```bash
geocompare export profile "New York city, New York"
geocompare export profile "New York city, New York" --profile-view compact
```

## Output formats

Where supported:

- `--format table`
- `--format json`
- `--format csv`
- `--wide`

## Help

```bash
geocompare --help
geocompare query --help
geocompare query similar --help
```

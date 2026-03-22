# Argument Types

## Data identifiers

Used by:

```bash
geocompare query top <data_identifier>
geocompare query bottom <data_identifier>
geocompare query remoteness <data_identifier> <threshold>
geocompare query local-average <data_identifier>
geocompare export rows "<data_identifier_or_category> ..."
```

A data identifier is the canonical metric name used internally and on the CLI,
for example:

- `population`
- `median_household_income`
- `bachelors_degree_or_higher_pct`
- `violent_crime_rate`
- `registered_voters`

Use `_pct` suffixes for percentage-style identifiers.

## Categories for `export rows`

Examples:

- `:geography`
- `:population`
- `:race`
- `:education`
- `:income`
- `:housing`

Categories expand into groups of related identifiers when exporting tabular rows.

## Display labels

Used by:

```bash
geocompare query profile "DISPLAY_LABEL"
geocompare query similar "DISPLAY_LABEL"
geocompare query nearest "DISPLAY_LABEL"
geocompare export profile "DISPLAY_LABEL"
```

A display label is the exact geography name string, for example:

- `New York city, New York`
- `Bethesda CDP, Maryland`
- `Queens County, New York`

If unsure, run `geocompare query search "name"` first.

## Scope

Used by:

```bash
--scope SCOPE
--universe UNIVERSE [--in-state ST | --in-county COUNTY | --in-zcta ZCTA]
```

Scope can be provided either as:

- a compact scope string via `--scope`
- explicit scope options via `--universe` plus at most one `--in-*` selector

Compact scope forms:

- `universe+`
- `universe+group`
- `group`

### Supported universes

- `nations` / `nation` / `n`
- `states` / `s`
- `counties` / `c`
- `tracts` / `tract` / `t`
- `places` / `p`
- `cbsas` / `cb`
- `urbanareas` / `u`
- `zctas` / `z`

Examples:

- `states+`
- `counties+ut`
- `tracts+ca`
- `places+ca`
- `zctas+94103`
- `ny`
- `06075:county`

Equivalent explicit scope examples:

```bash
--universe tracts --in-state ca
--universe places --in-state ca
--universe counties --in-county 06075:county
--universe counties --in-county "Los Angeles County, California"
--universe zctas --in-zcta 94103
```

## Filters

Used by:

```bash
--where EXPR
--match-where EXPR
```

For `query remoteness`:

- `--where` filters candidate geographies only
- `--match-where` filters qualifying geographies only
- `--county-population-min` and `--county-density-min` filter candidate geographies only

Supported operators:

- `>`
- `>=`
- `=`
- `<=`
- `<`

Multiple criteria can be joined with spaces, `,`, or `+`.

Examples:

```bash
geocompare query top median_rooms --where "population>=100000,median_rent<=1000" --universe places
geocompare query top population --where "graduate_degree_or_higher>=150000" --universe places
geocompare query remoteness median_household_income 100000 --universe tracts --where "population>=4000 population_density>=2500"
```

When using shell metacharacters like `>` or `<`, quote the expression.

## Row count

Used by:

```bash
-n N
```

`N` controls the number of rows returned.

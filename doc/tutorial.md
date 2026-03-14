# Tutorial

First, complete [Setup](./setup.md).

## Goal

This walkthrough covers a practical workflow:

1. Find places with large homes.
2. Narrow by geography and population.
3. Inspect one place in detail.
4. Find demographically similar places.
5. Search by name when you do not know the exact display label.

## 1. Rank places by median rooms

```bash
geocompare query top median_rooms --universe states
```

## 2. Narrow to one state or universe

```bash
geocompare query top median_rooms --universe counties --in-state ut
geocompare query top median_rooms --universe counties
geocompare query top median_rooms --universe counties --where "population>=1000000"
```

## 3. Inspect one geography

```bash
geocompare query profile "Fairfax County, Virginia"
```

## 4. Find demographically similar geographies

```bash
geocompare query similar "Fairfax County, Virginia"
```

`query similar` uses `GeoVector` distance, not physical distance.

## 5. Search when the label is uncertain

```bash
geocompare query search "Oconomowoc"
```

Then copy the exact display label into `query profile` or `query similar`.

## Next steps

- [Argument Types](./argument-types.md)
- [Commands](./commands.md)
- [Architecture](./architecture.md)

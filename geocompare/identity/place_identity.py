import re
import unicodedata

from rapidfuzz import fuzz


class PlaceIdentityIndex:
    """Resolve user place strings to canonical geography identifiers."""

    _ws_re = re.compile(r"\s+")
    _punct_re = re.compile(r"[^a-z0-9\s]")

    def __init__(self, entries):
        self.entries = entries
        self.by_norm = {}
        for entry in entries:
            norm = entry["norm_name"]
            self.by_norm.setdefault(norm, []).append(entry)

    @classmethod
    def from_demographic_profiles(cls, demographic_profiles):
        entries = []
        for dp in demographic_profiles:
            geoid = getattr(dp, "geoid", None)
            canonical_id = f"census:{geoid}" if geoid else f"name:{cls.normalize_name(dp.name)}"
            alias_values = [dp.name]
            canonical_name = getattr(dp, "canonical_name", None)
            if canonical_name:
                alias_values.append(canonical_name)
            if geoid:
                alias_values.extend([geoid, geoid.split("US", 1)[1] if "US" in geoid else geoid])

            seen_norms = set()
            for alias in alias_values:
                norm_name = cls.normalize_name(alias)
                if not norm_name or norm_name in seen_norms:
                    continue
                seen_norms.add(norm_name)
                entries.append(
                    {
                        "canonical_id": canonical_id,
                        "name": dp.name,
                        "canonical_name": canonical_name,
                        "norm_name": norm_name,
                        "state": dp.state,
                        "sumlevel": dp.sumlevel,
                        "population": dp.rc.get("population") if hasattr(dp, "rc") else None,
                        "geoid": geoid,
                    }
                )

        return cls(entries)

    @classmethod
    def normalize_name(cls, value):
        if value is None:
            return ""

        value = unicodedata.normalize("NFKD", str(value))
        value = value.encode("ascii", "ignore").decode("ascii")
        value = value.lower()

        # Remove trailing state/metadata portions common in Census display labels.
        value = value.replace(";", ",")
        if "," in value:
            value = value.split(",")[0]

        for token in [
            " census tract",
            " tract",
            " city",
            " town",
            " village",
            " borough",
            " municipality",
            " cdp",
            " county",
            " census designated place",
        ]:
            if value.endswith(token):
                value = value[: -len(token)]
                break

        value = cls._punct_re.sub(" ", value)
        value = cls._ws_re.sub(" ", value).strip()
        return value

    def _score(self, query, entry, state=None, sumlevel=None, population=None):
        score = fuzz.token_set_ratio(query, entry["norm_name"])

        if state and entry["state"] == state:
            score += 25
        if sumlevel and entry["sumlevel"] == sumlevel:
            score += 10

        if population is not None and entry.get("population"):
            pop = entry["population"]
            if pop > 0 and population > 0:
                diff_ratio = abs(pop - population) / max(population, pop)
                score += max(0, 15 - diff_ratio * 15)

        return score

    def resolve(self, query, state=None, sumlevel=None, population=None, limit=5):
        norm_query = self.normalize_name(query)
        if not norm_query:
            return []

        # Fast exact-normalized hit.
        exact = self.by_norm.get(norm_query)
        if exact:
            results = exact
        else:
            # Restrict candidate set using token containment if possible.
            tokens = [t for t in norm_query.split(" ") if t]
            candidates = []
            for entry in self.entries:
                norm = entry["norm_name"]
                if all(token in norm for token in tokens):
                    candidates.append(entry)

            if not candidates:
                candidates = self.entries

            scored = [
                (
                    self._score(
                        norm_query, entry, state=state, sumlevel=sumlevel, population=population
                    ),
                    entry,
                )
                for entry in candidates
            ]
            scored.sort(key=lambda item: item[0], reverse=True)
            results = [entry for _, entry in scored[: max(limit, 1)]]

        # If we got exact hits and filters are provided, rank exacts too.
        ranked = [
            (
                self._score(
                    norm_query, entry, state=state, sumlevel=sumlevel, population=population
                ),
                entry,
            )
            for entry in results
        ]
        ranked.sort(key=lambda item: item[0], reverse=True)

        return [entry for _, entry in ranked[: max(limit, 1)]]

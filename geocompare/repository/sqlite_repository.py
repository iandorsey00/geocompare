import hashlib
import heapq
import json
import logging
import math
import sqlite3
import zlib
from datetime import datetime, timezone
from pathlib import Path

from rapidfuzz import fuzz

from geocompare.repository.base import DataRepository
from geocompare.repository.serialization import dump_payload, load_payload

CURRENT_SCHEMA_VERSION = 1
_COMPRESSED_PAYLOAD_PREFIX = b"Z1:"


class SQLiteRepository(DataRepository):
    """Data repository backed by a SQLite file."""

    CURRENT_SCHEMA_VERSION = CURRENT_SCHEMA_VERSION

    def __init__(self, path):
        self.path = Path(path)
        self.logger = logging.getLogger(__name__)

    @property
    def name(self):
        return f"sqlite:{self.path}"

    def _connect(self):
        return sqlite3.connect(str(self.path))

    def _initialize(self, conn):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL
            )
            """
        )
        row = conn.execute("SELECT version FROM schema_version WHERE id = 1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO schema_version (id, version) VALUES (1, ?)",
                (self.CURRENT_SCHEMA_VERSION,),
            )
        else:
            current = int(row[0])
            if current > self.CURRENT_SCHEMA_VERSION:
                raise RuntimeError(
                    f"unsupported sqlite schema version {current}; "
                    f"max supported is {self.CURRENT_SCHEMA_VERSION}"
                )
            if current < self.CURRENT_SCHEMA_VERSION:
                self._migrate_schema(conn, current, self.CURRENT_SCHEMA_VERSION)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS data_products (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload BLOB NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

    def _migrate_schema(self, conn, from_version, to_version):
        # Migrations are intentionally explicit and step-based to keep
        # upgrades deterministic as the storage model evolves.
        version = from_version
        while version < to_version:
            next_version = version + 1
            self.logger.info(
                "migrating sqlite schema from v%s to v%s",
                version,
                next_version,
            )
            conn.execute(
                "UPDATE schema_version SET version = ? WHERE id = 1",
                (next_version,),
            )
            version = next_version

    def _is_numeric(self, value):
        return isinstance(value, (int, float))

    def _normalize_value(self, value):
        if value is None:
            return None
        if self._is_numeric(value) and math.isnan(value):
            return None
        return value

    def _sql_operator(self, operator_key):
        mapping = {
            "gt": ">",
            "gteq": ">=",
            "eq": "=",
            "lteq": "<=",
            "lt": "<",
        }
        op = mapping.get(operator_key)
        if op is None:
            raise RuntimeError(f"unsupported operator for SQL filter: {operator_key}")
        return op

    def _table_columns(self, conn, table_name):
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {row[1] for row in rows}

    def _ensure_column(self, conn, table_name, column_name):
        columns = self._table_columns(conn, table_name)
        if column_name not in columns:
            raise RuntimeError(f"column not found for {table_name}: {column_name}")

    def _index_name_for_column(self, column_name):
        digest = hashlib.sha1(column_name.encode("utf-8")).hexdigest()[:10]
        return f"idx_dp_comp_{digest}"

    def _ensure_order_index(self, conn, column_name):
        self._ensure_column(conn, "demographic_profiles", column_name)
        index_name = self._index_name_for_column(column_name)
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON demographic_profiles({column_name})"
        )

    def _build_profile_where_sql(
        self,
        conn,
        universe_sl=None,
        group_sl=None,
        group=None,
        county_geoid=None,
        geofilter_conditions=None,
        exclude_null_column=None,
        exclude_values=None,
    ):
        geofilter_conditions = geofilter_conditions or []
        exclude_values = exclude_values or []

        where = []
        params = []

        if exclude_null_column:
            self._ensure_column(conn, "demographic_profiles", exclude_null_column)
            where.append(f"{exclude_null_column} IS NOT NULL")

        if universe_sl:
            where.append("sumlevel = ?")
            params.append(universe_sl)

        if group_sl == "040":
            where.append("state = ?")
            params.append(group)
        elif group_sl == "860":
            where.append("name LIKE ?")
            params.append(f"ZCTA5 {group}%")
        elif group_sl == "050" and county_geoid:
            where.append("counties_geoids LIKE ?")
            params.append(f"%|{county_geoid}|%")

        for condition in geofilter_conditions:
            self._ensure_column(conn, "demographic_profiles", condition["column"])
            op = self._sql_operator(condition["operator"])
            where.append(f"{condition['column']} {op} ?")
            params.append(condition["value"])

        if exclude_null_column and exclude_values:
            placeholders = ", ".join(["?"] * len(exclude_values))
            where.append(f"{exclude_null_column} NOT IN ({placeholders})")
            params.extend(exclude_values)

        where_sql = "1 = 1" if not where else " AND ".join(where)
        return where_sql, params

    def _rebuild_profile_tables(self, conn, data_products):
        dps = data_products.get("demographicprofiles", [])
        gvs = data_products.get("geovectors", [])

        rc_keys = sorted({key for dp in dps for key in dp.rc.keys()})
        c_keys = sorted({key for dp in dps for key in dp.c.keys()})

        rc_columns = [f"rc_{key} REAL" for key in rc_keys]
        c_columns = [f"c_{key} REAL" for key in c_keys]

        conn.execute("DROP TABLE IF EXISTS demographic_profiles")
        conn.execute(
            f"""
            CREATE TABLE demographic_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                geoid TEXT,
                sumlevel TEXT NOT NULL,
                state TEXT NOT NULL,
                counties_geoids TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                population REAL,
                payload BLOB NOT NULL
                {"," if (rc_columns or c_columns) else ""}
                {", ".join(rc_columns + c_columns)}
            )
            """
        )

        conn.execute("CREATE INDEX idx_dp_name ON demographic_profiles(name)")
        conn.execute("CREATE INDEX idx_dp_geoid ON demographic_profiles(geoid)")
        conn.execute("CREATE INDEX idx_dp_sumlevel ON demographic_profiles(sumlevel)")
        conn.execute("CREATE INDEX idx_dp_state ON demographic_profiles(state)")
        conn.execute("CREATE INDEX idx_dp_population ON demographic_profiles(population)")
        conn.execute("CREATE INDEX idx_dp_latlon ON demographic_profiles(latitude, longitude)")

        column_names = (
            [
                "name",
                "geoid",
                "sumlevel",
                "state",
                "counties_geoids",
                "latitude",
                "longitude",
                "population",
                "payload",
            ]
            + [f"rc_{key}" for key in rc_keys]
            + [f"c_{key}" for key in c_keys]
        )

        placeholders = ", ".join(["?"] * len(column_names))
        insert_sql = (
            f'INSERT INTO demographic_profiles({", ".join(column_names)}) '
            f"VALUES ({placeholders})"
        )

        rows = []
        for dp in dps:
            counties_geoids = ""
            if getattr(dp, "counties", None):
                counties_geoids = f'|{"|".join(dp.counties)}|'

            row = [
                dp.name,
                getattr(dp, "geoid", None),
                dp.sumlevel,
                dp.state,
                counties_geoids,
                self._normalize_value(dp.rc.get("latitude")),
                self._normalize_value(dp.rc.get("longitude")),
                self._normalize_value(dp.rc.get("population")),
                dump_payload(dp),
            ]

            for key in rc_keys:
                row.append(self._normalize_value(dp.rc.get(key)))
            for key in c_keys:
                row.append(self._normalize_value(dp.c.get(key)))

            rows.append(row)

        if rows:
            conn.executemany(insert_sql, rows)

        conn.execute("DROP TABLE IF EXISTS geovectors")
        conn.execute(
            """
            CREATE TABLE geovectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                sumlevel TEXT NOT NULL,
                state TEXT NOT NULL,
                payload BLOB NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX idx_gv_name ON geovectors(name)")
        conn.execute("CREATE INDEX idx_gv_sumlevel ON geovectors(sumlevel)")
        conn.execute("CREATE INDEX idx_gv_state ON geovectors(state)")

        gv_rows = [
            (
                gv.name,
                gv.sumlevel,
                gv.state,
                dump_payload(gv),
            )
            for gv in gvs
        ]
        if gv_rows:
            conn.executemany(
                "INSERT INTO geovectors(name, sumlevel, state, payload) VALUES (?, ?, ?, ?)",
                gv_rows,
            )

    def save_data_products(self, data_products):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = dump_payload(data_products)
        payload = _COMPRESSED_PAYLOAD_PREFIX + zlib.compress(payload, level=6)
        updated_at = datetime.now(timezone.utc).isoformat()

        conn = self._connect()
        try:
            self._initialize(conn)
            conn.execute(
                """
                INSERT INTO data_products (id, payload, updated_at)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    payload=excluded.payload,
                    updated_at=excluded.updated_at
                """,
                (payload, updated_at),
            )
            self._rebuild_profile_tables(conn, data_products)
            conn.commit()
        finally:
            conn.close()

    def load_data_products(self):
        if not self.path.exists():
            raise RuntimeError(f"data product file not found: {self.path}")

        conn = self._connect()
        try:
            self._initialize(conn)
            row = conn.execute("SELECT payload FROM data_products WHERE id = 1").fetchone()
        finally:
            conn.close()

        if row is None:
            raise RuntimeError(f"no data products found in sqlite file: {self.path}")

        try:
            payload = row[0]
            if isinstance(payload, (bytes, bytearray)) and payload.startswith(_COMPRESSED_PAYLOAD_PREFIX):
                payload = zlib.decompress(payload[len(_COMPRESSED_PAYLOAD_PREFIX) :])
            return load_payload(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            raise RuntimeError(f"data product payload is corrupted or incompatible: {self.path}")
        except Exception as e:
            raise RuntimeError(f"unexpected error loading sqlite data products: {e!r}")

    def get_demographic_profile(self, display_label):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT payload
                FROM demographic_profiles
                WHERE name = ?
                ORDER BY population DESC
                LIMIT 1
                """,
                (display_label,),
            ).fetchone()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while loading profile: {e!r}")
        finally:
            conn.close()

        if row is None:
            return None

        try:
            payload = row[0]
            if isinstance(payload, (bytes, bytearray)) and payload.startswith(_COMPRESSED_PAYLOAD_PREFIX):
                payload = zlib.decompress(payload[len(_COMPRESSED_PAYLOAD_PREFIX) :])
            return load_payload(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, zlib.error):
            raise RuntimeError(f"profile payload is corrupted or incompatible: {self.path}")
        except Exception as e:
            raise RuntimeError(f"unexpected error loading sqlite profile: {e!r}")

    def get_demographic_profile_by_geoid(self, geoid):
        conn = self._connect()
        try:
            columns = self._table_columns(conn, "demographic_profiles")
            if "geoid" in columns:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM demographic_profiles
                    WHERE geoid = ? OR geoid LIKE ?
                    ORDER BY population DESC
                    LIMIT 1
                    """,
                    (geoid, f"%US{geoid}"),
                ).fetchone()
            else:
                row = None
                for candidate in conn.execute(
                    """
                    SELECT payload
                    FROM demographic_profiles
                    ORDER BY population DESC
                    """
                ):
                    payload = candidate[0]
                    if isinstance(payload, (bytes, bytearray)) and payload.startswith(_COMPRESSED_PAYLOAD_PREFIX):
                        payload = zlib.decompress(payload[len(_COMPRESSED_PAYLOAD_PREFIX) :])
                    profile = load_payload(payload)
                    candidate_geoid = getattr(profile, "geoid", None)
                    if candidate_geoid == geoid or str(candidate_geoid or "").endswith(str(geoid)):
                        row = candidate
                        break
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while loading profile by geoid: {e!r}")
        finally:
            conn.close()

        if row is None:
            return None

        try:
            payload = row[0]
            if isinstance(payload, (bytes, bytearray)) and payload.startswith(_COMPRESSED_PAYLOAD_PREFIX):
                payload = zlib.decompress(payload[len(_COMPRESSED_PAYLOAD_PREFIX) :])
            return load_payload(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, zlib.error):
            raise RuntimeError(f"profile payload is corrupted or incompatible: {self.path}")
        except Exception as e:
            raise RuntimeError(f"unexpected error loading sqlite profile by geoid: {e!r}")

    def get_any_demographic_profile(self):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT payload
                FROM demographic_profiles
                ORDER BY population DESC
                LIMIT 1
                """
            ).fetchone()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while loading sample profile: {e!r}")
        finally:
            conn.close()

        if row is None:
            return None

        return load_payload(row[0])

    def search_demographic_profiles(self, query, n):
        if n <= 0:
            return []

        tokens = [token for token in str(query or "").split() if token]
        if not tokens:
            return []

        conn = self._connect()
        try:
            where_sql = " AND ".join(["name LIKE ?"] * len(tokens))
            params = [f"%{token}%" for token in tokens]
            rows = conn.execute(
                f"""
                SELECT name
                FROM demographic_profiles
                WHERE {where_sql}
                LIMIT 5000
                """,
                params,
            ).fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while searching profiles: {e!r}")
        finally:
            conn.close()

        if not rows:
            return []

        best_names = [row[0] for row in heapq.nlargest(n, rows, key=lambda row: fuzz.token_set_ratio(query, row[0]))]
        return [self.get_demographic_profile(name) for name in best_names if self.get_demographic_profile(name) is not None]

    def get_coordinates(self, display_label):
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT latitude, longitude
                FROM demographic_profiles
                WHERE name = ?
                """,
                (display_label,),
            ).fetchone()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while loading coordinates: {e!r}")
        finally:
            conn.close()

        if row is None or row[0] is None or row[1] is None:
            return None
        return (row[0], row[1])

    def query_extreme_profile_names(
        self,
        comp_column,
        universe_sl=None,
        group_sl=None,
        group=None,
        county_geoid=None,
        geofilter_conditions=None,
        n=10,
        lowest=False,
        exclude_values=None,
    ):
        if n <= 0:
            return []

        conn = self._connect()
        try:
            self._ensure_order_index(conn, comp_column)
            where_sql, params = self._build_profile_where_sql(
                conn,
                universe_sl=universe_sl,
                group_sl=group_sl,
                group=group,
                county_geoid=county_geoid,
                geofilter_conditions=geofilter_conditions,
                exclude_null_column=comp_column,
                exclude_values=exclude_values,
            )

            order = "ASC" if lowest else "DESC"
            query = f"""
                SELECT DISTINCT name
                FROM demographic_profiles
                WHERE {where_sql}
                ORDER BY {comp_column} {order}
                LIMIT ?
                """
            params.append(n)

            rows = conn.execute(query, params).fetchall()
            return [row[0] for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while querying extremes: {e!r}")
        finally:
            conn.close()

    def query_profile_names(
        self,
        universe_sl=None,
        group_sl=None,
        group=None,
        county_geoid=None,
        geofilter_conditions=None,
        n=0,
    ):
        conn = self._connect()
        try:
            where_sql, params = self._build_profile_where_sql(
                conn,
                universe_sl=universe_sl,
                group_sl=group_sl,
                group=group,
                county_geoid=county_geoid,
                geofilter_conditions=geofilter_conditions,
            )

            query = f"""
                SELECT DISTINCT name
                FROM demographic_profiles
                WHERE {where_sql}
                ORDER BY name
                """
            if n and n > 0:
                query += " LIMIT ?"
                params.append(n)

            rows = conn.execute(query, params).fetchall()
            return [row[0] for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while querying names: {e!r}")
        finally:
            conn.close()

    def query_profile_coordinates(
        self,
        universe_sl=None,
        group_sl=None,
        group=None,
        county_geoid=None,
        geofilter_conditions=None,
        exclude_name=None,
        min_latitude=None,
        max_latitude=None,
        min_longitude=None,
        max_longitude=None,
        n=0,
    ):
        conn = self._connect()
        try:
            where_sql, params = self._build_profile_where_sql(
                conn,
                universe_sl=universe_sl,
                group_sl=group_sl,
                group=group,
                county_geoid=county_geoid,
                geofilter_conditions=geofilter_conditions,
            )

            where_sql += " AND latitude IS NOT NULL AND longitude IS NOT NULL"
            if exclude_name:
                where_sql += " AND name != ?"
                params.append(exclude_name)
            if min_latitude is not None:
                where_sql += " AND latitude >= ?"
                params.append(min_latitude)
            if max_latitude is not None:
                where_sql += " AND latitude <= ?"
                params.append(max_latitude)
            if min_longitude is not None:
                where_sql += " AND longitude >= ?"
                params.append(min_longitude)
            if max_longitude is not None:
                where_sql += " AND longitude <= ?"
                params.append(max_longitude)

            query = f"""
                SELECT name, latitude, longitude
                FROM demographic_profiles
                WHERE {where_sql}
                """
            if n and n > 0:
                query += " LIMIT ?"
                params.append(n)

            return conn.execute(query, params).fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while querying coordinates: {e!r}")
        finally:
            conn.close()

    def query_profile_metric_rows(
        self,
        comp_column,
        universe_sl=None,
        group_sl=None,
        group=None,
        county_geoid=None,
        geofilter_conditions=None,
        include_counties_geoids=False,
    ):
        conn = self._connect()
        try:
            self._ensure_column(conn, "demographic_profiles", comp_column)
            where_sql, params = self._build_profile_where_sql(
                conn,
                universe_sl=universe_sl,
                group_sl=group_sl,
                group=group,
                county_geoid=county_geoid,
                geofilter_conditions=geofilter_conditions,
                exclude_null_column=comp_column,
            )
            select_columns = ["name", "latitude", "longitude", "population", comp_column]
            if include_counties_geoids:
                select_columns.append("counties_geoids")
            query = f"""
                SELECT {", ".join(select_columns)}
                FROM demographic_profiles
                WHERE {where_sql}
                  AND latitude IS NOT NULL
                  AND longitude IS NOT NULL
            """
            return conn.execute(query, params).fetchall()
        except sqlite3.Error as e:
            raise RuntimeError(f"unexpected sqlite error while querying metric rows: {e!r}")
        finally:
            conn.close()

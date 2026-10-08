"""Tests for PostgreSQL schema introspection.

The important invariant is the query count: introspection must issue a fixed
number of set-based catalog queries, not one per relation (and certainly not one
per column). The per-relation version took ~1s per table against a remote
server, which pushed server startup past the client's handshake timeout.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

from pg_mcp.db.introspection import SchemaIntrospector


class _FakeConnection:
    """Serves canned catalog rows and records every query issued."""

    def __init__(self, dataset: dict[str, list[dict[str, Any]]]) -> None:
        self.dataset = dataset
        self.queries: list[str] = []

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.queries.append(query)
        if "version()" in query:
            return "PostgreSQL 16.15 on x86_64-pc-linux-gnu, compiled by gcc"
        raise AssertionError(f"unexpected fetchval: {query[:80]}")

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.queries.append(query)
        flat = " ".join(query.split())

        # Order matters: check the most specific markers first.
        if "NOT idx.indisprimary" in flat:
            return self.dataset["indexes"]
        if "i.indisprimary" in flat:
            return self.dataset["primary_keys"]
        if "reltuples::bigint" in flat:
            return self.dataset["row_counts"]
        if "con.contype = 'u'" in flat:
            return self.dataset["unique_columns"]
        if "con.contype = 'f'" in flat:
            return self.dataset["foreign_keys"]
        if "NOT a.attisdropped" in flat:
            return self.dataset["columns"]
        if "typtype = 'e'" in flat:
            return self.dataset["enums"]
        if "c.relkind = 'r'" in flat:
            return self.dataset["tables"]
        if "c.relkind = 'v'" in flat:
            return self.dataset["views"]
        raise AssertionError(f"unrecognised query: {flat[:120]}")


class _FakePool:
    """Minimal pool stand-in exposing `acquire()` as an async context manager."""

    def __init__(self, conn: _FakeConnection) -> None:
        self._conn = conn

    def acquire(self) -> Any:
        conn = self._conn

        @asynccontextmanager
        async def _acquire() -> AsyncIterator[_FakeConnection]:
            yield conn

        return _acquire()


def _build_dataset(table_count: int) -> dict[str, list[dict[str, Any]]]:
    """Build catalog rows for `table_count` identical two-column tables."""
    tables: list[dict[str, Any]] = []
    columns: list[dict[str, Any]] = []
    primary_keys: list[dict[str, Any]] = []
    unique_columns: list[dict[str, Any]] = []
    row_counts: list[dict[str, Any]] = []

    for i in range(table_count):
        name = f"t{i}"
        tables.append({"schema_name": "public", "table_name": name, "comment": None})
        columns.append(
            {
                "schema_name": "public",
                "table_name": name,
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": False,
                "default_value": None,
                "comment": None,
            }
        )
        columns.append(
            {
                "schema_name": "public",
                "table_name": name,
                "column_name": "name",
                "data_type": "text",
                "is_nullable": True,
                "default_value": None,
                "comment": None,
            }
        )
        primary_keys.append({"schema_name": "public", "table_name": name, "column_name": "id"})
        unique_columns.append({"schema_name": "public", "table_name": name, "column_name": "name"})
        row_counts.append({"schema_name": "public", "table_name": name, "estimate": i})

    return {
        "tables": tables,
        "views": [],
        "enums": [],
        "columns": columns,
        "primary_keys": primary_keys,
        "unique_columns": unique_columns,
        "foreign_keys": [],
        "indexes": [],
        "row_counts": row_counts,
    }


async def _introspect(table_count: int) -> tuple[Any, list[str]]:
    conn = _FakeConnection(_build_dataset(table_count))
    schema = await SchemaIntrospector(_FakePool(conn), "testdb").introspect()  # type: ignore[arg-type]
    return schema, conn.queries


class TestQueryCountIsBounded:
    """Regression: introspection must not scale its query count with the schema."""

    async def test_query_count_is_constant_as_tables_grow(self) -> None:
        _, small_queries = await _introspect(1)
        _, large_queries = await _introspect(50)

        assert len(small_queries) == len(large_queries)
        assert len(large_queries) <= 10, (
            f"introspection issued {len(large_queries)} queries for 50 tables; "
            "it should issue a fixed handful of set-based catalog queries"
        )

    async def test_query_count_ignores_column_count(self) -> None:
        """Columns must not each cost a round trip (the old N+1)."""
        dataset = _build_dataset(1)
        dataset["columns"] = [
            {
                "schema_name": "public",
                "table_name": "t0",
                "column_name": f"c{i}",
                "data_type": "text",
                "is_nullable": True,
                "default_value": None,
                "comment": None,
            }
            for i in range(40)
        ]
        conn = _FakeConnection(dataset)
        await SchemaIntrospector(_FakePool(conn), "testdb").introspect()  # type: ignore[arg-type]

        assert len(conn.queries) <= 10


class TestMetadataIsAttributedToTheRightTable:
    """The grouping by (schema, table) is the substance of the refactor."""

    async def test_columns_primary_keys_and_uniques(self) -> None:
        schema, _ = await _introspect(3)

        assert [t.table_name for t in schema.tables] == ["t0", "t1", "t2"]

        for i, table in enumerate(schema.tables):
            assert table.schema_name == "public"
            assert [c.name for c in table.columns] == ["id", "name"]
            assert table.row_count_estimate == i

            id_col, name_col = table.columns
            assert id_col.is_primary_key is True
            assert id_col.is_unique is False, "primary key must not also be flagged as UNIQUE"
            assert name_col.is_primary_key is False
            assert name_col.is_unique is True

    async def test_tables_are_not_leaked_between_schemas(self) -> None:
        """A same-named table in another schema must not receive these columns."""
        dataset = _build_dataset(1)
        dataset["tables"] = [
            {"schema_name": "public", "table_name": "t0", "comment": None},
            {"schema_name": "archive", "table_name": "t0", "comment": None},
        ]
        conn = _FakeConnection(dataset)
        schema = await SchemaIntrospector(_FakePool(conn), "testdb").introspect()  # type: ignore[arg-type]

        by_schema = {t.schema_name: t for t in schema.tables}
        assert [c.name for c in by_schema["public"].columns] == ["id", "name"]
        assert by_schema["archive"].columns == []

    async def test_version_is_reported(self) -> None:
        schema, _ = await _introspect(1)
        assert schema.version == "PostgreSQL 16.15 on x86_64-pc-linux-gnu"
        assert schema.database_name == "testdb"


@pytest.mark.parametrize("table_count", [0, 1, 5])
async def test_empty_and_small_schemas_do_not_error(table_count: int) -> None:
    schema, _ = await _introspect(table_count)
    assert len(schema.tables) == table_count

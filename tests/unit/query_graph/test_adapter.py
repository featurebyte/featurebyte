import textwrap
from typing import cast

import pytest
from sqlglot import expressions, parse_one
from sqlglot.expressions import Select

from featurebyte.enum import SourceType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from tests.util.helper import assert_sql_equal, get_sql_adapter_from_source_type


@pytest.mark.parametrize(
    "source_type, expected",
    [
        (
            SourceType.SNOWFLAKE,
            'CREATE TABLE "db1"."schema1"."table1" AS SELECT * FROM A',
        ),
        (
            SourceType.SPARK,
            "CREATE TABLE `db1`.`schema1`.`table1` USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
        (
            SourceType.DATABRICKS,
            "CREATE TABLE `db1`.`schema1`.`table1` USING DELTA TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.minReaderVersion'='2', 'delta.minWriterVersion'='5') AS SELECT * FROM A",
        ),
    ],
)
def test_create_table_as(source_type, expected):
    """
    Test create_table_as for Adapter
    """

    table_details = TableDetails(
        database_name="db1",
        schema_name="schema1",
        table_name="table1",
    )
    expr = parse_one("SELECT * FROM A")
    new_expr = get_sql_adapter_from_source_type(source_type).create_table_as(
        table_details, cast(Select, expr)
    )
    assert new_expr.sql(dialect=source_type).strip() == expected


@pytest.mark.parametrize(
    "source_type, expected",
    [
        (
            SourceType.SNOWFLAKE,
            textwrap.dedent(
                """
                ALTER TABLE my_database.my_schema.my_table ADD COLUMN "my_col_1" BIGINT,
                "my_col_2" TIMESTAMPNTZ
                """
            ).strip(),
        ),
        (
            SourceType.DATABRICKS,
            "ALTER TABLE my_database.my_schema.my_table ADD COLUMNS (`my_col_1` LONG, `my_col_2` TIMESTAMP)",
        ),
    ],
)
def test_alter_table_add_columns(source_type, expected):
    """
    Test alter_table_add_columns
    """
    result = get_sql_adapter_from_source_type(source_type).alter_table_add_columns(
        table=expressions.Table(
            this="my_table",
            db="my_schema",
            catalog="my_database",
        ),
        columns=[
            expressions.ColumnDef(
                this=quoted_identifier("my_col_1"),
                kind=expressions.DataType.build("BIGINT"),
            ),
            expressions.ColumnDef(
                this=quoted_identifier("my_col_2"),
                kind=expressions.DataType.build("TIMESTAMP"),
            ),
        ],
    )
    assert result == expected


@pytest.mark.parametrize(
    "source_type, expected",
    [
        (
            SourceType.SNOWFLAKE,
            'OBJECT_AGG("key_col", TO_VARIANT("value_col"))',
        ),
        (
            SourceType.DATABRICKS,
            """
            MAP_FILTER(
              MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(`key_col`, `value_col`))),
              (k, v) -> NOT v IS NULL
            )
            """,
        ),
        (
            SourceType.SPARK,
            """
            MAP_FILTER(
              MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(`key_col`, `value_col`))),
              (k, v) -> NOT v IS NULL
            )
            """,
        ),
        (
            SourceType.BIGQUERY,
            """
            CASE
              WHEN ARRAY_AGG(`value_col`) IS NULL
              THEN JSON_OBJECT()
              ELSE JSON_STRIP_NULLS(JSON_OBJECT(ARRAY_AGG(`key_col`), ARRAY_AGG(`value_col`)))
            END
            """,
        ),
    ],
)
def test_object_agg(source_type, expected):
    """
    Test object_agg
    """
    result = get_sql_adapter_from_source_type(source_type).object_agg(
        key_column=quoted_identifier("key_col"),
        value_column=quoted_identifier("value_col"),
    )
    assert_sql_equal(sql_to_string(result, source_type), expected)

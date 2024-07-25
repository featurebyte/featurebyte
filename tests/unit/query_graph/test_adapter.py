import textwrap
from typing import cast

import pytest
from sqlglot import expressions, parse_one
from sqlglot.expressions import Select

from featurebyte.enum import SourceType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier


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
    new_expr = get_sql_adapter(source_type).create_table_as(table_details, cast(Select, expr))
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
    result = get_sql_adapter(source_type).alter_table_add_columns(
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

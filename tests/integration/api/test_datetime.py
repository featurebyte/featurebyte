"""
Integration tests for datetime operations
"""

import pandas as pd
import pytest
from sqlglot import expressions
from sqlglot.expressions import alias_

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.ast.literal import make_literal_value


@pytest.mark.asyncio
async def test_datetime_timestamp_difference(catalog, session, data_source, source_type):
    _ = catalog
    table_name = "TEST_DATETIME_TIMESTAMP_DIFF"
    table_expr = expressions.select(
        alias_(make_literal_value(1), alias="dimension_id", quoted=True),
        alias_(
            expressions.Cast(
                this=make_literal_value("2024-01-01 12:00:00"),
                to=expressions.DataType.build("TIMESTAMP"),
            ),
            alias="d_ntz",
            quoted=True,
        ),
        alias_(
            expressions.Cast(
                this=make_literal_value("2024-01-01 10:00:00"),
                to=expressions.DataType.build("TIMESTAMPTZ"),
            ),
            alias="d_tz",
            quoted=True,
        ),
        alias_(
            expressions.Cast(
                this=make_literal_value("2024-01-01"),
                to=expressions.DataType.build("DATE"),
            ),
            alias="d_dt",
            quoted=True,
        ),
    )
    await session.create_table_as(
        table_details=table_name,
        select_expr=table_expr,
    )
    database_table = data_source.get_source_table(
        database_name=session.database_name,
        schema_name=session.schema_name,
        table_name=table_name,
    )
    dimension_table = database_table.create_dimension_table(
        name=table_name,
        dimension_id_column="dimension_id",
    )
    view = dimension_table.get_view()
    view["ntz_tz"] = (view["d_ntz"] - view["d_tz"]).dt.second
    view["ntz_dt"] = (view["d_ntz"] - view["d_dt"]).dt.second
    view["tz_dt"] = (view["d_tz"] - view["d_dt"]).dt.second
    df = view.preview()
    if source_type == SourceType.SNOWFLAKE:
        expected_d_tz = pd.Timestamp("2024-01-01 10:00:00+0000", tz="UTC")
    else:
        expected_d_tz = pd.Timestamp("2024-01-01 10:00:00")
    if source_type in [SourceType.SPARK, SourceType.DATABRICKS_UNITY]:
        expected_d_dt = "2024-01-01"
    else:
        expected_d_dt = "2024-01-01T00:00:00.000000000"
    expected = [
        {
            "dimension_id": 1,
            "d_ntz": pd.Timestamp("2024-01-01 12:00:00"),
            "d_tz": expected_d_tz,
            "d_dt": expected_d_dt,
            "ntz_tz": 7200.0,
            "ntz_dt": 43200.0,
            "tz_dt": 36000.0,
        }
    ]
    assert df.to_dict(orient="records") == expected

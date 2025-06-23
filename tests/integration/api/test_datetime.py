"""
Integration tests for datetime operations
"""

from datetime import datetime

import pandas as pd
import pytest
from sqlglot import expressions
from sqlglot.expressions import alias_

from featurebyte import (
    AddTimestampSchema,
    FeatureList,
    RequestColumn,
    TimestampSchema,
    to_timestamp_from_epoch,
)
from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from tests.util.deployment import deploy_and_get_online_features
from tests.util.helper import fb_assert_frame_equal


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


def test_to_timestamp_from_epoch(event_table, config, session):
    """
    Test to_timestamp_from_epoch
    """
    view = event_table.get_view()
    view["epoch_second"] = 1712006400
    view["converted_timestamp"] = to_timestamp_from_epoch(view["epoch_second"])
    df = view.preview()
    assert (df["converted_timestamp"] == pd.Timestamp("2024-04-01 21:20:00")).all()

    feature_name = "test_to_timestamp_from_epoch_feature"
    view["timestamp_hour"] = view["converted_timestamp"].dt.hour
    feature = view.groupby("ÜSER ID", category="timestamp_hour").aggregate_over(
        value_column=None,
        method="count",
        windows=["14d"],
        feature_names=[feature_name],
    )

    # Test preview
    preview_param = {
        "POINT_IN_TIME": "2001-01-02 10:00:00",
        "üser id": 1,
    }
    feature_list = FeatureList([feature], name=f"{feature_name}_list")
    df = feature_list.preview(pd.DataFrame([preview_param]))
    expected = {
        "POINT_IN_TIME": pd.Timestamp("2001-01-02 10:00:00"),
        "üser id": 1,
        feature_name: {"21": 24},
    }
    assert df.iloc[0].to_dict() == expected

    # Test online serving
    df_feat = deploy_and_get_online_features(
        config.get_client(),
        feature_list,
        datetime(2001, 1, 2, 10),
        [{"üser id": 1}],
    )
    df_expected = pd.DataFrame([
        {
            "üser id": 1,
            "test_to_timestamp_from_epoch_feature": '{\n  "21": 2.300000000000000e+01\n}',
        }
    ])
    fb_assert_frame_equal(
        df_feat,
        df_expected,
        dict_like_columns=["test_to_timestamp_from_epoch_feature"],
    )


def test_date_difference_of_varchar_timestamp_with_timezone(event_table, config, session):
    """
    Test date difference of varchar timestamp with timezone
    """
    source_type = session.source_type
    if source_type in SourceType.java_time_format_types():
        format_string = "yyyy-dd-MM HHmmss"
    elif source_type == SourceType.BIGQUERY:
        format_string = "%Y-%d-%m %H%M%S"
    elif source_type == SourceType.SNOWFLAKE:
        format_string = "YYYY-DD-MM HH24MISS"
    else:
        raise ValueError("Unexpected source type")

    event_table.TIMESTAMP_STRING.update_critical_data_info(
        cleaning_operations=[
            AddTimestampSchema(
                timestamp_schema=TimestampSchema(
                    is_utc_time=False,
                    format_string=format_string,
                    timezone="Asia/Singapore",
                )
            )
        ]
    )
    view = event_table.get_view()
    feat = view.groupby(by_keys=["ÜSER ID"]).aggregate_over(
        value_column="TIMESTAMP_STRING",
        method="min",
        windows=["2d"],
        feature_names=["min_timestamp_string_2d"],
    )["min_timestamp_string_2d"]

    diff_in_day = (RequestColumn.point_in_time() - feat).dt.day
    diff_in_day.name = "diff_in_day"

    # Test preview
    preview_param = {
        "POINT_IN_TIME": "2001-06-15 10:00:00",
        "üser id": 1,
    }

    feature_list = FeatureList([diff_in_day, feat], name="test_timestamp_schema_list")
    preview_df = feature_list.preview(pd.DataFrame([preview_param]))
    expected = {
        "POINT_IN_TIME": pd.Timestamp(preview_param["POINT_IN_TIME"]),
        "üser id": preview_param["üser id"],
        "diff_in_day": -80.125,
        "min_timestamp_string_2d": "2001-03-09 210000",
    }
    assert preview_df.iloc[0].to_dict() == expected

    # Test online serving
    request_row = {"üser id": 1}
    df_feat = deploy_and_get_online_features(
        config.get_client(),
        feature_list,
        datetime(2001, 6, 15, 10),
        [request_row],
    )
    assert df_feat.iloc[0].to_dict() == {
        **request_row,
        "diff_in_day": -80.125,
        "min_timestamp_string_2d": "2001-03-09 210000",
    }

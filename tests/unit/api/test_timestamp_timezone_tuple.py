"""
Unit tests for timestamp with timezone offset tuple data type
"""

import textwrap

import pandas as pd
import pytest

from featurebyte import CalendarWindow, CronFeatureJobSetting, RequestColumn, to_timedelta
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.dtype import DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampTupleSchema,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.node.date import DatetimeExtractNodeParameters


@pytest.fixture(name="feat_timestamp_tz_tuple")
def fixture_feat_timestamp_tz_tuple(snowflake_time_series_table_with_tz_offset_column):
    """Return a feature with TIMESTAMP_TZ_TUPLE data type"""
    view = snowflake_time_series_table_with_tz_offset_column.get_view()
    feature = view.groupby("store_id").aggregate_over(
        value_column="date",
        method="latest",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        feature_names=["col_float_sum_3month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
        ),
    )["col_float_sum_3month"]
    return feature


def test_dtype_info(feat_timestamp_tz_tuple, snowflake_time_series_table_with_tz_offset_column):
    """Test feature dtype_info"""
    view = snowflake_time_series_table_with_tz_offset_column.get_view()
    dtype_info = feat_timestamp_tz_tuple.operation_structure.series_output_dtype_info
    assert dtype_info.dtype == DBVarType.TIMESTAMP_TZ_TUPLE
    assert dtype_info.timestamp_schema is None
    assert dtype_info.metadata.timestamp_tuple_schema.timestamp_schema == ExtendedTimestampSchema(
        dtype=DBVarType.VARCHAR,
        **view["date"].dtype_info.metadata.timestamp_schema.model_dump(),
    )
    assert (
        dtype_info.metadata.timestamp_tuple_schema.timezone_offset_schema
        == TimezoneOffsetSchema(dtype=DBVarType.VARCHAR)
    )


def test_save_feature(feat_timestamp_tz_tuple):
    """Test saving feature with TIMESTAMP_TZ_TUPLE data type"""
    # feature should not be saved
    with pytest.raises(NotImplementedError) as exc_info:
        feat_timestamp_tz_tuple.save()
    expected_msg = "Feature with data type TIMESTAMP_TZ_TUPLE is not supported for saving."
    assert str(exc_info.value) == expected_msg


def test_dt_accessor(feat_timestamp_tz_tuple, snowflake_time_series_table_with_tz_offset_column):
    """Test aggregate_over over time series column with timezone offset column"""
    # add post-processing to save the feature
    view = snowflake_time_series_table_with_tz_offset_column.get_view()
    feat_day = feat_timestamp_tz_tuple.dt.day
    feat_day.name = "feat_day"
    feat_day.save()

    graph = feat_day.cached_model.graph
    dt_extract_node = graph.get_node_by_name("dt_extract_1")
    assert dt_extract_node.parameters == DatetimeExtractNodeParameters(
        property="day",
        timezone_offset=None,
        timestamp_metadata=DBVarTypeMetadata(
            timestamp_schema=None,
            timestamp_tuple_schema=TimestampTupleSchema(
                timestamp_schema=ExtendedTimestampSchema(
                    dtype="VARCHAR",
                    **view["date"].dtype_info.metadata.timestamp_schema.model_dump(),
                ),
                timezone_offset_schema=TimezoneOffsetSchema(dtype="VARCHAR"),
            ),
        ),
    )

    partial_definition = textwrap.dedent("""
        view["__date_zip_timezone"] = col_1.zip_timestamp_timezone_columns()
        grouped = view.groupby(by_keys=["store_id"], category=None).aggregate_over(
            value_column="__date_zip_timezone",
            method="latest",
            windows=[CalendarWindow(unit="MONTH", size=3)],
            feature_names=["col_float_sum_3month"],
            feature_job_setting=CronFeatureJobSetting(
                crontab=Crontab(
                    minute=0, hour=8, day_of_month=1, month_of_year="*", day_of_week="*"
                ),
                timezone="Etc/UTC",
                reference_timezone=None,
                blind_spot=None,
            ),
            skip_fill_na=True,
            offset=None,
        )
    """).strip()
    assert partial_definition in feat_day.definition


def test_date_diff(feat_timestamp_tz_tuple):
    """Test date different"""
    feat1 = (RequestColumn.point_in_time() - feat_timestamp_tz_tuple).dt.day
    feat1.name = "feat_date_diff"
    feat1.save()

    feat2 = (feat_timestamp_tz_tuple - RequestColumn.point_in_time()).dt.day
    feat2.name = "another_feat_date_diff"
    feat2.save()


def test_date_add(feat_timestamp_tz_tuple):
    """Test date add"""
    with pytest.raises(NotImplementedError) as exc_info:
        feat_timestamp_tz_tuple + pd.Timedelta(days=1)

    expected_msg = "Date add operation is not supported for TIMESTAMP_TZ_TUPLE data type."
    assert str(exc_info.value) == expected_msg

    feat_timedelta = to_timedelta(feat_timestamp_tz_tuple.dt.day, "day")
    with pytest.raises(NotImplementedError) as exc_info:
        feat_timestamp_tz_tuple + feat_timedelta
    assert str(exc_info.value) == expected_msg

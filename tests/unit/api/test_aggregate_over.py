"""
Unit tests for aggregate_over
"""

from typing import Any

import pytest
from bson import ObjectId

from featurebyte import Crontab
from featurebyte.enum import DBVarType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import (
    CronFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.model.window import CalendarWindow
from tests.util.helper import get_node


@pytest.mark.parametrize(
    "value_column, expected_dtype",
    [
        ("col_int", DBVarType.INT),
        ("col_float", DBVarType.FLOAT),
        ("col_char", DBVarType.CHAR),
        ("col_text", DBVarType.VARCHAR),
        ("col_boolean", DBVarType.BOOL),
        ("event_timestamp", DBVarType.TIMESTAMP_TZ),
        ("col_binary", DBVarType.BINARY),
    ],
)
def test_aggregate_over__latest_method_output_vartype(
    snowflake_event_view_with_entity, value_column, expected_dtype
):
    """
    Test latest aggregation output variable type
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column=value_column,
        method="latest",
        windows=["1h"],
        feature_names=["feat_1h"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    assert feature_group["feat_1h"].dtype == expected_dtype


def test_unbounded_window__valid(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test a valid case of specifying None as window size
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    feature_dict = feature_group["feat_latest"].model_dump()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["windows"] == [None]


def test_unbounded_window__non_latest(snowflake_event_view_with_entity):
    """
    Test window size of None is only valid for latest aggregation method
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
        )
    assert str(exc.value) == 'Unbounded window is only supported for the "latest" method'


def test_unbounded_window__category_not_supported(snowflake_event_view_with_entity):
    """
    Test aggregation per category is not supported with unbounded windows
    """
    with pytest.raises(ValueError) as exc:
        snowflake_event_view_with_entity.groupby("cust_id", category="col_int").aggregate_over(
            value_column="col_float",
            method="latest",
            windows=[None],
            feature_names=["feat_latest"],
            feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
        )
    assert str(exc.value) == "category is not supported for aggregation with unbounded window"


def test_unbounded_window__composite_keys(snowflake_event_view_with_entity):
    """
    Test composite keys
    """
    feature_group = snowflake_event_view_with_entity.groupby(["cust_id", "col_int"]).aggregate_over(
        value_column="col_float",
        method="latest",
        windows=[None],
        feature_names=["feat_latest"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="6m", offset="3m"),
    )
    feature_dict = feature_group["feat_latest"].model_dump()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["windows"] == [None]
    assert node["parameters"]["keys"] == ["cust_id", "col_int"]


def test_empty_groupby_keys(snowflake_event_view_with_entity):
    """
    Test empty groupby keys (feature without any entity)
    """
    feature_group = snowflake_event_view_with_entity.groupby([]).aggregate_over(
        value_column=None,
        method="count",
        windows=["30d"],
        feature_names=["feat_count"],
        feature_job_setting=FeatureJobSetting(blind_spot="1m30s", period="60m", offset="3m"),
    )
    feature_dict = feature_group["feat_count"].model_dump()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["keys"] == []
    assert node["parameters"]["entity_ids"] == []

    feature_model = FeatureModel(**feature_group["feat_count"].model_dump())
    assert feature_model.entity_ids == []


def test_offset(snowflake_event_view_with_entity):
    """
    Test offset with window aggregation
    """
    feature_group = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="max",
        windows=["24h"],
        offset="12h",
        feature_names=["feature"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
        ),
    )
    feature_dict = feature_group["feature"].model_dump()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["offset"] == "12h"


def test_offset__invalid_duration(snowflake_event_view_with_entity):
    """
    Test offset with window aggregation
    """
    with pytest.raises(ValueError) as exc_info:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_float",
            method="max",
            windows=["24h"],
            offset="13m",
            feature_names=["feature"],
            feature_job_setting=FeatureJobSetting(
                blind_spot="1m30s", frequency="6m", time_modulo_frequency="3m"
            ),
        )
    assert (
        str(exc_info.value)
        == "window provided 13m is not a multiple of the feature job frequency 360s"
    )


def test_count_distinct_agg_func(snowflake_event_view_with_entity, cust_id_entity):
    """
    Test count_distinct aggregation function produces a different node type
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="count_distinct",
        windows=["7d"],
        feature_names=["col_int_count_distinct_7d"],
        feature_job_setting=FeatureJobSetting(blind_spot="0", period="1d", offset="1h"),
    )["col_int_count_distinct_7d"]

    feature_node_name = feature.node.name
    aggregation_node_name = feature.graph.backward_edges_map[feature_node_name][0]
    aggregation_node = feature.graph.get_node_by_name(aggregation_node_name)
    assert aggregation_node.type == NodeType.NON_TILE_WINDOW_AGGREGATE
    assert aggregation_node.parameters.model_dump() == {
        "agg_func": "count_distinct",
        "entity_ids": [cust_id_entity.id],
        "feature_job_setting": {
            "blind_spot": "0s",
            "execution_buffer": "0s",
            "offset": "3600s",
            "period": "86400s",
        },
        "keys": ["cust_id"],
        "names": ["col_int_count_distinct_7d"],
        "offset": None,
        "parent": "col_int",
        "serving_names": ["cust_id"],
        "timestamp": "event_timestamp",
        "timestamp_metadata": None,
        "value_by": None,
        "windows": ["7d"],
    }

    # check feature can be saved
    feature.save()


@pytest.mark.parametrize("is_int_type_crontab", [True, False])
def test_time_series_view_aggregate_over(
    snowflake_time_series_table_with_entity, is_int_type_crontab
):
    """
    Test aggregate_over for time series view
    """
    snowflake_time_series_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(
            crontab=Crontab(
                minute=0 if is_int_type_crontab else "0",
                hour=1 if is_int_type_crontab else "1",
                day_of_week="*",
                day_of_month="*",
                month_of_year="*",
            ),
            timezone="Etc/UTC",
        )
    )
    view = snowflake_time_series_table_with_entity.get_view()
    feature = view.groupby("store_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        feature_names=["col_float_sum_3month"],
    )["col_float_sum_3month"]
    feature_dict = feature.model_dump()
    node = get_node(feature_dict["graph"], "time_series_window_aggregate_1")
    assert node == {
        "name": "time_series_window_aggregate_1",
        "type": "time_series_window_aggregate",
        "output_type": "frame",
        "parameters": {
            "keys": ["store_id"],
            "parent": "col_float",
            "agg_func": "sum",
            "value_by": None,
            "serving_names": ["cust_id"],
            "entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
            "windows": [{"unit": "MONTH", "size": 3}],
            "reference_datetime_column": "date",
            "reference_datetime_metadata": {
                "timestamp_schema": {
                    "format_string": "YYYY-MM-DD HH24:MI:SS",
                    "is_utc_time": None,
                    "timezone": "Etc/UTC",
                },
                "timestamp_tuple_schema": None,
            },
            "time_interval": {"unit": "DAY", "value": 1},
            "names": ["col_float_sum_3month"],
            "feature_job_setting": {
                "crontab": {
                    "minute": 0 if is_int_type_crontab else "0",
                    "hour": 1 if is_int_type_crontab else "1",
                    "day_of_month": "*",
                    "month_of_year": "*",
                    "day_of_week": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": "Etc/UTC",
                "blind_spot": None,
            },
            "offset": None,
        },
    }

    # check feature can be saved
    feature.save()


def test_time_series_view_aggregate_over__only_cron_feature_job_setting(
    snowflake_time_series_view_with_entity,
):
    """
    Test aggregate_over for time series view only accepts CronFeatureJobSetting
    """
    view = snowflake_time_series_view_with_entity
    with pytest.raises(ValueError) as exc_info:
        _ = view.groupby("store_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=[CalendarWindow(unit="MONTH", size=3)],
            feature_names=["col_float_sum_3month"],
            feature_job_setting=FeatureJobSetting(blind_spot="0", period="1d", offset="1h"),
        )
    assert (
        str(exc_info.value)
        == "feature_job_setting must be CronFeatureJobSetting for calendar aggregation"
    )


@pytest.mark.parametrize("is_offset", [True, False])
def test_time_series_view_aggregate_over__only_feature_window(
    snowflake_time_series_view_with_entity, is_offset
):
    """
    Test validation of windows and offset for time series view aggregate_over
    """
    view = snowflake_time_series_view_with_entity
    valid_window = CalendarWindow(unit="MONTH", size=3)
    invalid_window = "3d"
    args: dict[str, Any] = dict(
        value_column="col_float",
        method="sum",
        windows=[valid_window],
        offset=valid_window,
        feature_names=["col_float_sum_3month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
        ),
    )
    # Invalidate either offset or windows
    if is_offset:
        args["offset"] = invalid_window
        expected = "Please specify offset as CalendarWindow for calendar aggregation"
    else:
        args["windows"] = [invalid_window]
        args["feature_job_setting"] = FeatureJobSetting(
            blind_spot="0",
            period="1d",
            offset="1h",
        )
        expected = "CalendarWindow is only supported for calendar aggregation"
    with pytest.raises(ValueError) as exc_info:
        _ = view.groupby("store_id").aggregate_over(**args)
    assert str(exc_info.value) == expected


@pytest.mark.parametrize(
    "window_unit, is_allowed",
    [
        ("MINUTE", False),
        ("HOUR", False),
        ("DAY", True),
        ("MONTH", True),
        ("QUARTER", True),
        ("YEAR", True),
    ],
)
def test_time_series_view_aggregate_over__validate_window_unit(
    snowflake_time_series_view_with_entity, window_unit, is_allowed
):
    """
    Test aggregate_over for time series view validates the compatibility between table's time
    interval and window unit
    """
    view = snowflake_time_series_view_with_entity

    def do_aggregate_over():
        _ = view.groupby("store_id").aggregate_over(
            value_column="col_float",
            method="sum",
            windows=[CalendarWindow(unit=window_unit, size=3)],
            feature_names=["col_float_sum"],
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 8 1 * *",
            ),
        )

    if not is_allowed:
        with pytest.raises(ValueError) as exc_info:
            do_aggregate_over()
        assert (
            str(exc_info.value)
            == f"Window unit ({window_unit}) cannot be smaller than the table's time interval unit (DAY)"
        )
    else:
        do_aggregate_over()


def test_time_series_view_aggregate_over__same_unit_windows_and_offset(
    snowflake_time_series_view_with_entity,
):
    """
    Test validation of windows and offset for time series view aggregate_over
    """
    view = snowflake_time_series_view_with_entity
    args: dict[str, Any] = dict(
        value_column="col_float",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        offset=CalendarWindow(unit="DAY", size=7),
        feature_names=["col_float_sum_3month"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
        ),
    )
    with pytest.raises(ValueError) as exc_info:
        _ = view.groupby("store_id").aggregate_over(**args)
    assert str(exc_info.value) == "Window unit (MONTH) must be the same as the offset unit (DAY)"


@pytest.mark.parametrize("is_offset", [True, False])
def test_non_time_series_view_aggregate_over__only_str_window(
    snowflake_event_view_with_entity, is_offset
):
    """
    Test validation of windows and offset for non-time series view aggregate_over
    """
    view = snowflake_event_view_with_entity
    valid_window = "3d"
    invalid_window = CalendarWindow(unit="MONTH", size=3)
    args: dict[str, Any] = dict(
        value_column="col_int",
        method="count_distinct",
        windows=[valid_window],
        offset=valid_window,
        feature_names=["col_int_count_distinct_7d"],
        feature_job_setting=FeatureJobSetting(blind_spot="0", period="1d", offset="1h"),
    )
    # Invalidate either offset or windows
    if is_offset:
        args["offset"] = invalid_window
        expected = "CalendarWindow is only supported for calendar aggregation"
    else:
        args["windows"] = [invalid_window]
        expected = "Please specify offset as CalendarWindow for calendar aggregation"
    with pytest.raises(ValueError) as exc_info:
        _ = view.groupby("cust_id").aggregate_over(**args)
    assert str(exc_info.value) == expected


def test_non_time_series_view_aggregate_over__only_str_window__non_cron_feature_job_setting(
    snowflake_event_view_with_entity,
):
    """
    Test non-time series view aggregate_over only accepts FeatureJobSetting
    """
    with pytest.raises(ValueError) as exc_info:
        _ = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
            value_column="col_int",
            method="count_distinct",
            windows=["7d"],
            feature_names=["col_int_count_distinct_7d"],
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 8 1 * *",
            ),
        )
    assert (
        str(exc_info.value)
        == "feature_job_setting cannot be used for non-calendar aggregation (blind_spot is not specified)"
    )


def test_event_view_calendar_window_aggregate(snowflake_event_view_with_entity):
    """
    Test calendar aggregation for event view
    """
    feature = snowflake_event_view_with_entity.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=[CalendarWindow(unit="MONTH", size=3)],
        feature_names=["col_int_sum_3m"],
        feature_job_setting=CronFeatureJobSetting(
            crontab="0 8 1 * *",
        ),
    )["col_int_sum_3m"]

    feature_dict = feature.model_dump()
    node = get_node(feature_dict["graph"], "time_series_window_aggregate_1")
    assert node == {
        "name": "time_series_window_aggregate_1",
        "type": "time_series_window_aggregate",
        "output_type": "frame",
        "parameters": {
            "keys": ["cust_id"],
            "parent": "col_float",
            "agg_func": "sum",
            "value_by": None,
            "serving_names": ["cust_id"],
            "entity_ids": [ObjectId("63f94ed6ea1f050131379214")],
            "windows": [{"unit": "MONTH", "size": 3}],
            "reference_datetime_column": "event_timestamp",
            "reference_datetime_metadata": None,
            "time_interval": None,
            "names": ["col_int_sum_3m"],
            "feature_job_setting": {
                "crontab": {
                    "minute": 0,
                    "hour": 8,
                    "day_of_month": 1,
                    "month_of_year": "*",
                    "day_of_week": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": None,
                "blind_spot": None,
            },
            "offset": None,
        },
    }

    # check feature can be saved
    feature.save()


def test_aggregate_over__default_cron_feature_job_setting(
    event_table_with_cron_feature_job_setting,
):
    """
    Test default cron feature job setting is automatically converted when doing non-calendar aggregation
    """
    event_view = event_table_with_cron_feature_job_setting.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column=None,
        method="count",
        windows=["1d"],
        feature_names=["count_1d"],
    )["count_1d"]
    feature_dict = feature.model_dump()
    node = get_node(feature_dict["graph"], "groupby_1")
    assert node["parameters"]["feature_job_setting"] == {
        "blind_spot": "600s",
        "period": "86400s",
        "offset": "0s",
        "execution_buffer": "0s",
    }
    feature.save()

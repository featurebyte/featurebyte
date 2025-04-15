"""
Tests for datetime operations in the SDK
"""

import textwrap

import freezegun
import pytest

from featurebyte import AddTimestampSchema, FeatureList, RequestColumn, TimestampSchema
from featurebyte.core.datetime import to_timestamp_from_epoch
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.util.helper import assert_equal_with_expected_fixture


def test_timestamp_diff_with_schema(snowflake_time_series_view_with_entity, update_fixtures):
    """
    Test difference between timestamps with TimestampSchema
    """
    view = snowflake_time_series_view_with_entity
    view["new_col"] = view["date"] - view["another_timestamp_col"]
    preview_sql = view.preview_sql()
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/api/test_datetime_operations/timestamp_diff_with_schema.sql",
        update_fixtures,
    )


def test_to_timestamp_from_epoch(snowflake_event_view_with_entity, update_fixtures):
    """
    Test to_timestamp_from_epoch
    """
    view = snowflake_event_view_with_entity
    view["converted_timestamp"] = to_timestamp_from_epoch(view["col_int"])
    view["converted_timestamp_hour"] = view["converted_timestamp"].dt.hour
    preview_sql = view.preview_sql()
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/api/test_datetime_operations/to_timestamp_from_epoch.sql",
        update_fixtures,
    )
    feature = view.groupby("cust_id").aggregate_over(
        value_column="converted_timestamp_hour",
        method="max",
        windows=["2d"],
        feature_names=["max_hour_2d"],
        feature_job_setting=FeatureJobSetting(
            blind_spot="1h",
            period="1h",
            offset="0s",
        ),
    )["max_hour_2d"]
    feature.save()


def test_to_timestamp_from_epoch__non_numeric(snowflake_event_view_with_entity, update_fixtures):
    """
    Test to_timestamp_from_epoch
    """
    view = snowflake_event_view_with_entity
    with pytest.raises(ValueError) as exc_info:
        view["converted_timestamp"] = to_timestamp_from_epoch(view["col_text"])
    assert (
        str(exc_info.value) == "to_timestamp_from_epoch requires input to be numeric, got VARCHAR"
    )


def test_date_diff_with_varchar_timestamp(
    saved_event_table,
    cust_id_entity,
    snowflake_time_series_table_with_tz_offset_column,
    arbitrary_default_feature_job_setting,
    mock_deployment_flow,
):
    """
    Test date diff with varchar timestamp
    """
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    cleaning_operations = [
        AddTimestampSchema(
            timestamp_schema=TimestampSchema(
                is_utc_time=False,
                format_string="YY-MM-DD HH24:MI",
                timezone="Asia/Singapore",
            )
        )
    ]
    saved_event_table.col_text.update_critical_data_info(cleaning_operations=cleaning_operations)

    view = saved_event_table.get_view()
    assert view.col_text.is_datetime is True
    feat = view.groupby(by_keys=["cust_id"]).aggregate_over(
        value_column="col_text",
        method="max",
        windows=["2d"],
        feature_names=["max_col_text_2d"],
        feature_job_setting=arbitrary_default_feature_job_setting,
    )["max_col_text_2d"]

    assert feat.is_datetime is True
    time_diff = feat - RequestColumn.point_in_time()
    assert time_diff.node.type == NodeType.DATE_DIFF

    diff_in_day = time_diff.dt.day
    diff_in_day.name = "diff_in_day"

    feature_list = FeatureList([diff_in_day], name="diff_in_day")
    with freezegun.freeze_time("2025-01-01"):
        feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    deployment.enable()

    odfv_info = diff_in_day.cached_model.offline_store_info.odfv_info
    codes = odfv_info.codes.replace(str(diff_in_day.id), "[FEATURE_ID]").strip()
    expected = textwrap.dedent(
        """
        import datetime
        import json
        import numpy as np
        import pandas as pd
        import scipy as sp


        def odfv_diff_in_day_v250101_[FEATURE_ID](
            inputs: pd.DataFrame,
        ) -> pd.DataFrame:
            df = pd.DataFrame()
            request_col = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)

            # TTL handling for __diff_in_day_V250101__part0 column
            request_time = pd.to_datetime(inputs["POINT_IN_TIME"], utc=True)
            cutoff = request_time - pd.Timedelta(seconds=1800)
            feat_ts = pd.to_datetime(
                inputs["__diff_in_day_V250101__part0__ts"], utc=True, unit="s"
            )
            mask = (feat_ts >= cutoff) & (feat_ts <= request_time)
            inputs.loc[~mask, "__diff_in_day_V250101__part0"] = np.nan
            feat = pd.to_datetime(
                inputs["__diff_in_day_V250101__part0"], format="%y-%m-%d %H:%M"
            ).dt.tz_localize("Asia/Singapore").dt.tz_convert("UTC") - pd.to_datetime(
                request_col, utc=True
            )
            feat_1 = pd.to_timedelta(feat).dt.total_seconds() // 86400
            df["diff_in_day_V250101"] = feat_1
            return df
        """
    ).strip()
    assert codes == expected, codes

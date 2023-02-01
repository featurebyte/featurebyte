"""
Unit tests for change view

Note that we don't currently inherit from the base view test suite as there are quite a few differences. I'll
work on updating that in a follow-up.
"""
import textwrap
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from featurebyte.api.change_view import ChangeView
from featurebyte.enum import SourceType
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


@pytest.fixture
def feature_from_change_view(snowflake_scd_data_with_entity):
    """
    Fixture for a feature created from a ChangeView
    """
    snowflake_change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data_with_entity, "col_int"
    )
    feature_group = snowflake_change_view.groupby("col_text").aggregate_over(
        method="count", windows=["30d"], feature_names=["feat_30d"]
    )
    return feature_group["feat_30d"]


def test_get_default_feature_job_setting():
    """
    Test get_default_feature_job_setting
    """
    # default is returned if nothing is provided
    datetime_mock = Mock(wraps=datetime)
    mocked_hour = 11
    mocked_minute = 15
    datetime_mock.now.return_value = datetime(1999, 1, 1, mocked_hour, mocked_minute, 0)
    with patch("featurebyte.api.change_view.datetime", new=datetime_mock):
        feature_job_setting = ChangeView.get_default_feature_job_setting()
        assert feature_job_setting == FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{mocked_hour}h{mocked_minute}m",
            frequency="24h",
        )

    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    # get back setting provided
    feature_job_setting = ChangeView.get_default_feature_job_setting(job_setting_provided)
    assert feature_job_setting == job_setting_provided


def test_validate_inputs(snowflake_scd_data):
    """
    Test _validate_inputs
    """
    # empty input should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_inputs(snowflake_scd_data, "")
    assert "Empty column provided" in str(exc_info)

    # column not in SCD data should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_inputs(snowflake_scd_data, "random_col")
    assert "Column provided is not a column in the SlowlyChangingData provided" in str(exc_info)

    # column in SCD data should be ok
    ChangeView._validate_inputs(snowflake_scd_data, "col_int")


def test_validate_prefixes():
    """
    Test _validate_prefixes
    """
    # No error expected
    ChangeView._validate_prefixes(None)

    # Both None should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes((None, None))
    assert "Prefixes provided are both None" in str(exc_info)

    # Empty string in second position should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("old", ""))
    assert "Please provide a non-empty string as a prefix value" in str(exc_info)

    # Empty string in first position should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("", "new"))
    assert "Please provide a non-empty string as a prefix value" in str(exc_info)

    # Same prefix should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView._validate_prefixes(("same_prefix", "same_prefix"))
    assert "Prefixes provided need to be different values" in str(exc_info)


def test_get_new_column_names():
    """
    Test _get_new_column_names
    """
    col_name = "col_name"
    old_col, new_col = ChangeView._get_new_column_names(col_name, None)
    assert old_col == f"past_{col_name}"
    assert new_col == f"new_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, (None, "updated_"))
    assert old_col == f"past_{col_name}"
    assert new_col == f"updated_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, ("prior_", None))
    assert old_col == f"prior_{col_name}"
    assert new_col == f"new_{col_name}"

    old_col, new_col = ChangeView._get_new_column_names(col_name, ("prior_", "updated_"))
    assert old_col == f"prior_{col_name}"
    assert new_col == f"updated_{col_name}"


def change_view_test_helper(snowflake_scd_data, change_view):
    """
    Helper method to do some asserts
    """
    assert len(change_view.columns_info) == 4
    assert change_view.timestamp_column == snowflake_scd_data.effective_timestamp_column
    assert change_view.natural_key_column == snowflake_scd_data.natural_key_column
    assert change_view.columns == ["col_text", "effective_timestamp", "new_col_int", "past_col_int"]


def test_from_scd_data__no_default_job_setting(snowflake_scd_data):
    """
    Test from_slowly_changing_data - no default job setting provided
    """
    datetime_mock = Mock(wraps=datetime)
    mocked_hour = 11
    mocked_minute = 15
    datetime_mock.now.return_value = datetime(1999, 1, 1, mocked_hour, mocked_minute, 0)
    with patch("featurebyte.api.change_view.datetime", new=datetime_mock):
        change_view = ChangeView.from_slowly_changing_data(snowflake_scd_data, "col_int")
        assert change_view.default_feature_job_setting == FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{mocked_hour}h{mocked_minute}m",
            frequency="24h",
        )
        change_view_test_helper(snowflake_scd_data, change_view)


def test_from_slowly_changing_data__with_default_job_setting(snowflake_scd_data):
    """
    Test from_slowly_changing_data - default job setting provided
    """
    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data, "col_int", job_setting_provided
    )
    assert change_view.default_feature_job_setting == job_setting_provided
    change_view_test_helper(snowflake_scd_data, change_view)


def test_update_feature_job_setting(snowflake_change_view):
    """
    Test update feature job setting
    """
    # Assert that a feature job setting exists
    assert snowflake_change_view.default_feature_job_setting is not None

    new_feature_job_setting = FeatureJobSetting(
        blind_spot="15m",
        time_modulo_frequency="30m",
        frequency="1h",
    )
    snowflake_change_view.update_default_feature_job_setting(new_feature_job_setting)
    assert snowflake_change_view.default_feature_job_setting == new_feature_job_setting


def test_aggregate_over_feature_tile_sql(feature_from_change_view):
    """
    Test tile sql is as expected for a feature created from ChangeView
    """
    pruned_graph, pruned_node = feature_from_change_view.extract_pruned_graph_and_node()
    interpreter = GraphInterpreter(pruned_graph, source_type=SourceType.SNOWFLAKE)
    tile_infos = interpreter.construct_tile_gen_sql(pruned_node, is_on_demand=False)
    assert len(tile_infos) == 1
    expected_aggregation_id = pruned_graph.get_node_by_name("groupby_1").parameters.aggregation_id
    expected = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 86400
          ) AS __FB_TILE_START_DATE_COLUMN,
          "col_text",
          COUNT(*) AS value_{expected_aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "effective_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 86400
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "col_text" AS "col_text",
                "effective_timestamp" AS "effective_timestamp"
              FROM "sf_database"."sf_schema"."scd_table"
            )
            WHERE
              "effective_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "effective_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "col_text"
        """
    ).strip()
    assert tile_infos[0].sql == expected

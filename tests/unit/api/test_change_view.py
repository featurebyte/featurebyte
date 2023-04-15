"""
Unit tests for change view

Note that we don't currently inherit from the base view test suite as there are quite a few differences. I'll
work on updating that in a follow-up.
"""
import textwrap
from datetime import datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from featurebyte.api.change_view import ChangeView
from featurebyte.api.entity import Entity
from featurebyte.core.frame import FrozenFrame
from featurebyte.core.series import FrozenSeries
from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    MissingValueImputation,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import check_sdk_code_generation


@pytest.fixture
def feature_from_change_view(saved_scd_table, cust_id_entity):
    """
    Fixture for a feature created from a ChangeView
    """
    saved_scd_table["col_text"].as_entity(cust_id_entity.name)
    snowflake_change_view = saved_scd_table.get_change_view("col_int")
    feature_group = snowflake_change_view.groupby("col_text").aggregate_over(
        method="count", windows=["30d"], feature_names=["feat_30d"]
    )
    feature = feature_group["feat_30d"]
    feature.save()
    return feature


@pytest.fixture(name="snowflake_scd_table_with_imputation")
def snowflake_scd_table_with_imputation_fixture(snowflake_scd_table_with_entity):
    """
    Fixture for a SCDTable with imputation
    """
    snowflake_scd_table_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=-1)]
    )
    return snowflake_scd_table_with_entity


def test_auto_view_mode(snowflake_scd_table_with_imputation):
    """
    Test auto view mode
    """
    # create view
    snowflake_change_view = snowflake_scd_table_with_imputation.get_change_view("col_int")

    # check view graph metadata
    metadata = snowflake_change_view.node.parameters.metadata
    assert snowflake_scd_table_with_imputation.record_creation_timestamp_column is None
    assert metadata.view_mode == "auto"
    assert metadata.drop_column_names == []
    assert metadata.column_cleaning_operations == [
        {
            "column_name": "col_int",
            "cleaning_operations": [{"imputed_value": -1, "type": "missing"}],
        }
    ]
    assert metadata.table_id == snowflake_scd_table_with_imputation.id

    # check that cleaning graph is created
    nested_graph = snowflake_change_view.node.parameters.graph
    cleaning_graph_node = nested_graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.type == "cleaning"

    # check SDK code generation
    check_sdk_code_generation(
        snowflake_change_view,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table_with_imputation.id: {
                "name": snowflake_scd_table_with_imputation.name,
                "record_creation_timestamp_column": snowflake_scd_table_with_imputation.record_creation_timestamp_column,
            }
        },
    )


def test_manual_view_mode(snowflake_scd_table_with_imputation):
    """
    Test manual view mode
    """
    # create view
    snowflake_change_view = snowflake_scd_table_with_imputation.get_change_view(
        "col_int", view_mode="manual"
    )

    # check view graph metadata
    metadata = snowflake_change_view.node.parameters.metadata
    assert metadata.view_mode == "manual"
    assert metadata.drop_column_names == []
    assert metadata.column_cleaning_operations == []
    assert metadata.table_id == snowflake_scd_table_with_imputation.id

    # check that there is no cleaning graph
    nested_graph = snowflake_change_view.node.parameters.graph
    assert all(not node_name.startswith("graph") for node_name in nested_graph.nodes_map.keys())

    # check SDK code generation
    check_sdk_code_generation(
        snowflake_change_view,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table_with_imputation.id: {
                "name": snowflake_scd_table_with_imputation.name,
                "record_creation_timestamp_column": snowflake_scd_table_with_imputation.record_creation_timestamp_column,
            }
        },
    )


def test_view_mode__auto_manual_equality_check(snowflake_scd_table_with_imputation):
    """
    Test view mode (create a view in auto mode, then create another equivalent view in manual mode).
    The equality is checked by comparing the view graphs. By using this relationship, we can
    reconstruct the view graph in manual mode from the view graph in auto mode.
    """
    # create view using auto mode
    view_auto = snowflake_scd_table_with_imputation.get_change_view("col_int")

    # create another equivalent view using manual mode
    snowflake_scd_table_with_imputation["col_int"].update_critical_data_info(cleaning_operations=[])
    drop_column_names = view_auto.node.parameters.metadata.drop_column_names
    view_manual = snowflake_scd_table_with_imputation.get_change_view(
        "col_int",
        view_mode="manual",
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="col_int",
                cleaning_operations=[MissingValueImputation(imputed_value=-1)],
            )
        ],
        drop_column_names=drop_column_names,
    )

    # check both view graph node inner graph are equal
    assert view_manual.node.parameters.graph == view_auto.node.parameters.graph
    assert (
        view_manual.node.parameters.output_node_name == view_auto.node.parameters.output_node_name
    )


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


def test_validate_inputs(snowflake_scd_table):
    """
    Test _validate_inputs
    """
    # empty input should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView.validate_inputs(snowflake_scd_table, "")
    assert "Empty column provided" in str(exc_info)

    # column not in SCD table should error
    with pytest.raises(ValueError) as exc_info:
        ChangeView.validate_inputs(snowflake_scd_table, "random_col")
    assert "Column provided is not a column in the SCDTable provided" in str(exc_info)

    # column in SCD table should be ok
    ChangeView.validate_inputs(snowflake_scd_table, "col_int")


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
    Test get_new_column_names
    """
    col_name = "col_name"
    timestamp_col_name = "timestamp_col"
    column_names = SCDTableData.get_new_column_names(col_name, timestamp_col_name, None)
    assert column_names.previous_tracked_column_name == f"past_{col_name}"
    assert column_names.new_tracked_column_name == f"new_{col_name}"
    assert column_names.previous_valid_from_column_name == f"past_{timestamp_col_name}"
    assert column_names.new_valid_from_column_name == f"new_{timestamp_col_name}"

    column_names = SCDTableData.get_new_column_names(
        col_name, timestamp_col_name, (None, "updated_")
    )
    assert column_names.previous_tracked_column_name == f"past_{col_name}"
    assert column_names.new_tracked_column_name == f"updated_{col_name}"
    assert column_names.previous_valid_from_column_name == f"past_{timestamp_col_name}"
    assert column_names.new_valid_from_column_name == f"updated_{timestamp_col_name}"

    column_names = SCDTableData.get_new_column_names(col_name, timestamp_col_name, ("prior_", None))
    assert column_names.previous_tracked_column_name == f"prior_{col_name}"
    assert column_names.new_tracked_column_name == f"new_{col_name}"
    assert column_names.previous_valid_from_column_name == f"prior_{timestamp_col_name}"
    assert column_names.new_valid_from_column_name == f"new_{timestamp_col_name}"

    column_names = SCDTableData.get_new_column_names(
        col_name, timestamp_col_name, ("prior_", "updated_")
    )
    assert column_names.previous_tracked_column_name == f"prior_{col_name}"
    assert column_names.new_tracked_column_name == f"updated_{col_name}"
    assert column_names.previous_valid_from_column_name == f"prior_{timestamp_col_name}"
    assert column_names.new_valid_from_column_name == f"updated_{timestamp_col_name}"


def change_view_test_helper(snowflake_scd_table, change_view):
    """
    Helper method to do some asserts
    """
    assert len(change_view.columns_info) == 5
    assert change_view.timestamp_column == "new_effective_timestamp"
    assert change_view.natural_key_column == snowflake_scd_table.natural_key_column
    assert change_view.columns == [
        "col_text",
        "new_effective_timestamp",
        "past_effective_timestamp",
        "new_col_int",
        "past_col_int",
    ]


def test_get_change_view__no_default_job_setting(snowflake_scd_table):
    """
    Test get_change_view - no default job setting provided
    """
    datetime_mock = Mock(wraps=datetime)
    mocked_hour = 11
    mocked_minute = 15
    datetime_mock.now.return_value = datetime(1999, 1, 1, mocked_hour, mocked_minute, 0)
    with patch("featurebyte.api.change_view.datetime", new=datetime_mock):
        change_view = snowflake_scd_table.get_change_view("col_int")
        assert change_view.default_feature_job_setting == FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{mocked_hour}h{mocked_minute}m",
            frequency="24h",
        )
        change_view_test_helper(snowflake_scd_table, change_view)


def test_get_change_view__with_default_job_setting(snowflake_scd_table):
    """
    Test get_change_view - default job setting provided
    """
    job_setting_provided = FeatureJobSetting(
        blind_spot="1h", time_modulo_frequency="1h", frequency="12h"
    )
    change_view = snowflake_scd_table.get_change_view("col_int", job_setting_provided)
    assert change_view.default_feature_job_setting == job_setting_provided
    change_view_test_helper(snowflake_scd_table, change_view)


def test_get_change_view__check_entity_id(snowflake_scd_table):
    """
    Test get_change_view:w
     - entity_id from the SCD table is correctly set
    """
    entity_key = Entity(name="key_column", serving_names=["key_column"])
    entity_eff_ts = Entity(name="eff_timestamp", serving_names=["eff_timestamp"])
    entity_change = Entity(name="change", serving_names=["change"])
    for entity in [entity_key, entity_eff_ts, entity_change]:
        entity.save()

    snowflake_scd_table[snowflake_scd_table.natural_key_column].as_entity("key_column")
    snowflake_scd_table[snowflake_scd_table.effective_timestamp_column].as_entity("eff_timestamp")
    snowflake_scd_table.col_int.as_entity("change")

    # create change view
    change_view = snowflake_scd_table.get_change_view("col_int")
    columns_info_dict = change_view.dict()["columns_info"]
    assert columns_info_dict == [
        {
            "critical_data_info": None,
            "dtype": "VARCHAR",
            "entity_id": entity_key.id,
            "name": "col_text",
            "semantic_id": columns_info_dict[0]["semantic_id"],
        },
        {
            "critical_data_info": None,
            "dtype": "TIMESTAMP_TZ",
            "entity_id": entity_eff_ts.id,
            "name": "new_effective_timestamp",
            "semantic_id": None,
        },
        {
            "critical_data_info": None,
            "dtype": "TIMESTAMP_TZ",
            "entity_id": None,
            "name": "past_effective_timestamp",
            "semantic_id": None,
        },
        {
            "critical_data_info": None,
            "dtype": "INT",
            "entity_id": entity_change.id,
            "name": "new_col_int",
            "semantic_id": columns_info_dict[3]["semantic_id"],
        },
        {
            "critical_data_info": None,
            "dtype": "INT",
            "entity_id": None,
            "name": "past_col_int",
            "semantic_id": None,
        },
    ]


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
                DATE_PART(EPOCH_SECOND, "new_effective_timestamp") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 86400
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "col_text" AS "col_text",
                "new_effective_timestamp" AS "new_effective_timestamp",
                "past_effective_timestamp" AS "past_effective_timestamp",
                "new_col_int" AS "new_col_int",
                "past_col_int" AS "past_col_int"
              FROM (
                SELECT
                  "col_text",
                  "new_effective_timestamp",
                  "new_col_int",
                  LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
                  "past_col_int"
                FROM (
                  SELECT
                    "col_text",
                    "effective_timestamp" AS "new_effective_timestamp",
                    "col_int" AS "new_col_int",
                    LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_col_int"
                  FROM (
                    SELECT
                      "col_int" AS "col_int",
                      "col_float" AS "col_float",
                      "is_active" AS "is_active",
                      "col_text" AS "col_text",
                      "col_binary" AS "col_binary",
                      "col_boolean" AS "col_boolean",
                      "effective_timestamp" AS "effective_timestamp",
                      "end_timestamp" AS "end_timestamp",
                      "created_at" AS "created_at",
                      "cust_id" AS "cust_id"
                    FROM "sf_database"."sf_schema"."scd_table"
                  )
                  QUALIFY
                    "col_int" IS DISTINCT FROM LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
                )
              )
            )
            WHERE
              "new_effective_timestamp" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "new_effective_timestamp" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "col_text"
        """
    ).strip()
    assert tile_infos[0].sql == expected


def test_get_change_view__keep_record_creation_timestamp_column(
    snowflake_scd_table, mock_api_object_cache
):
    """
    Test create ChangeView using record creation timestamp column as track changes column
    """
    snowflake_scd_table.update_record_creation_timestamp_column("created_at")
    assert snowflake_scd_table.record_creation_timestamp_column == "created_at"
    change_view = snowflake_scd_table.get_change_view(
        track_changes_column=snowflake_scd_table.record_creation_timestamp_column
    )
    expected_sql = textwrap.dedent(
        """
        SELECT
          "col_text" AS "col_text",
          CAST("new_effective_timestamp" AS STRING) AS "new_effective_timestamp",
          CAST("past_effective_timestamp" AS STRING) AS "past_effective_timestamp",
          CAST("new_created_at" AS STRING) AS "new_created_at",
          CAST("past_created_at" AS STRING) AS "past_created_at"
        FROM (
          SELECT
            "col_text",
            "new_effective_timestamp",
            "new_created_at",
            LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
            "past_created_at"
          FROM (
            SELECT
              "col_text",
              "effective_timestamp" AS "new_effective_timestamp",
              "created_at" AS "new_created_at",
              LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_created_at"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "is_active" AS "is_active",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "effective_timestamp" AS "effective_timestamp",
                "end_timestamp" AS "end_timestamp",
                "created_at" AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."scd_table"
            )
            QUALIFY
              "created_at" IS DISTINCT FROM LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
          )
        )
        LIMIT 10
        """
    ).strip()
    assert change_view.preview_sql() == expected_sql
    assert change_view.node.type == NodeType.GRAPH
    assert change_view.node.parameters.graph.edges[:1] == [
        {"source": "proxy_input_1", "target": "project_1"},  # no cleaning operation
    ]

    # check the case when the table has cleaning operations
    # cleaned table should be used to generate the change view
    snowflake_scd_table["created_at"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="2020-01-01")]
    )
    change_view = snowflake_scd_table.get_change_view(
        track_changes_column=snowflake_scd_table.record_creation_timestamp_column
    )
    expected_sql = textwrap.dedent(
        """
        SELECT
          "col_text" AS "col_text",
          CAST("new_effective_timestamp" AS STRING) AS "new_effective_timestamp",
          CAST("past_effective_timestamp" AS STRING) AS "past_effective_timestamp",
          CAST("new_created_at" AS STRING) AS "new_created_at",
          CAST("past_created_at" AS STRING) AS "past_created_at"
        FROM (
          SELECT
            "col_text",
            "new_effective_timestamp",
            "new_created_at",
            LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
            "past_created_at"
          FROM (
            SELECT
              "col_text",
              "effective_timestamp" AS "new_effective_timestamp",
              "created_at" AS "new_created_at",
              LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_created_at"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "is_active" AS "is_active",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "effective_timestamp" AS "effective_timestamp",
                "end_timestamp" AS "end_timestamp",
                CASE WHEN (
                  "created_at" IS NULL
                ) THEN '2020-01-01' ELSE "created_at" END AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."scd_table"
            )
            QUALIFY
              "created_at" IS DISTINCT FROM LAG("created_at") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
          )
        )
        LIMIT 10
        """
    ).strip()
    assert change_view.preview_sql() == expected_sql

    # check the change view graph node
    assert change_view.node.type == NodeType.GRAPH
    assert change_view.node.parameters.graph.edges[:2] == [
        {"source": "proxy_input_1", "target": "project_1"},
        {"source": "project_1", "target": "graph_1"},
    ]
    nested_graph_node = change_view.node.parameters.graph.get_node_by_name("graph_1")
    assert nested_graph_node.parameters.type == "cleaning"


def test_sdk_code_generation(saved_scd_table, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    change_view = saved_scd_table.get_change_view(
        track_changes_column="col_int",
        default_feature_job_setting=FeatureJobSetting(
            blind_spot="0", time_modulo_frequency="1h", frequency="24h"
        ),
        prefixes=(None, "_past"),
    )
    check_sdk_code_generation(
        change_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/change_view.py",
        update_fixtures=update_fixtures,
        table_id=saved_scd_table.id,
    )

    # check the case when the view is construct with cleaning operations
    change_view = saved_scd_table.get_change_view(
        track_changes_column="col_int",
        view_mode="manual",
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name="col_int",
                cleaning_operations=[MissingValueImputation(imputed_value=0)],
            )
        ],
    )
    check_sdk_code_generation(
        change_view,
        to_use_saved_data=to_use_saved_data,
        fixture_path="tests/fixtures/sdk_code/change_view_with_column_clean_ops.py",
        update_fixtures=update_fixtures,
        table_id=saved_scd_table.id,
    )


def test_raw_accessor(snowflake_scd_table):
    """Test raw accessor"""
    change_view = snowflake_scd_table.get_change_view("col_int")
    assert change_view.raw.node.type == NodeType.INPUT
    pd.testing.assert_series_equal(change_view.raw.dtypes, snowflake_scd_table.dtypes)

    # check read operation is ok
    column = "col_int"
    raw_subset_frame = change_view.raw[[column]]
    raw_subset_series = change_view.raw[column]
    assert isinstance(raw_subset_frame, FrozenFrame)
    assert isinstance(raw_subset_series, FrozenSeries)

    # check write operation is not allowed
    with pytest.raises(TypeError) as exc:
        change_view.raw[column] = 1
    assert "'FrozenFrame' object does not support item assignment" in str(exc.value)

    mask = change_view.raw[column] > 1
    with pytest.raises(TypeError) as exc:
        change_view.raw[column][mask] = 1
    assert "'FrozenSeries' object does not support item assignment" in str(exc.value)

    # check operation between raw input and view
    change_view[column] = change_view.raw[column] + 1
    expected_view_with_raw_accessor_sql = """
    SELECT
      "col_text" AS "col_text",
      CAST("effective_timestamp" AS STRING) AS "new_effective_timestamp",
      CAST(LAG("effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS STRING) AS "past_effective_timestamp",
      "col_int" AS "new_col_int",
      LAG("col_int", 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_col_int",
      (
        "col_int" + 1
      ) AS "col_int"
    FROM "sf_database"."sf_schema"."scd_table"
    LIMIT 10
    """
    assert (
        change_view.preview_sql().strip()
        == textwrap.dedent(expected_view_with_raw_accessor_sql).strip()
    )

    # conditional assignment
    change_view[column][mask] = 0
    assert change_view.node.type == NodeType.ASSIGN

    change_view[mask, column] = 0
    assert change_view.node.type == NodeType.ASSIGN

    # check filtering
    filtered_column = change_view[column][mask]
    assert filtered_column.node.type == NodeType.FILTER


def test_filtered_view_output(saved_scd_table, cust_id_entity):
    """
    Test filtering ChangeView
    """
    saved_scd_table["col_text"].as_entity(cust_id_entity.name)
    change_view = saved_scd_table.get_change_view("col_int")
    mask = change_view.new_col_int > 10
    filtered_view = change_view[mask]
    output_sql = filtered_view.preview_sql()
    expected_sql = """
    SELECT
      "col_text" AS "col_text",
      "new_effective_timestamp" AS "new_effective_timestamp",
      "past_effective_timestamp" AS "past_effective_timestamp",
      "new_col_int" AS "new_col_int",
      "past_col_int" AS "past_col_int"
    FROM (
      SELECT
        "col_text",
        "new_effective_timestamp",
        "new_col_int",
        LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
        "past_col_int"
      FROM (
        SELECT
          "col_text",
          "effective_timestamp" AS "new_effective_timestamp",
          "col_int" AS "new_col_int",
          LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_col_int"
        FROM (
          SELECT
            "col_int" AS "col_int",
            "col_float" AS "col_float",
            "is_active" AS "is_active",
            "col_text" AS "col_text",
            "col_binary" AS "col_binary",
            "col_boolean" AS "col_boolean",
            "effective_timestamp" AS "effective_timestamp",
            "end_timestamp" AS "end_timestamp",
            "created_at" AS "created_at",
            "cust_id" AS "cust_id"
          FROM "sf_database"."sf_schema"."scd_table"
        )
        QUALIFY
          "col_int" IS DISTINCT FROM LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
      )
    )
    WHERE
      (
        "new_col_int" > 10
      )
    LIMIT 10
    """
    assert output_sql.strip() == textwrap.dedent(expected_sql).strip()


def test_change_view_column_lag(snowflake_change_view):
    """
    Test applying lag for any columns in the ChangeView works
    """
    columns = snowflake_change_view.columns[:]
    for col in columns:
        snowflake_change_view[f"lag_{col}"] = snowflake_change_view[col].lag("col_text")
    expected = textwrap.dedent(
        """
        SELECT
          "col_text" AS "col_text",
          "new_effective_timestamp" AS "new_effective_timestamp",
          "past_effective_timestamp" AS "past_effective_timestamp",
          "new_col_int" AS "new_col_int",
          "past_col_int" AS "past_col_int",
          LAG("col_text", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_col_text",
          LAG("new_effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_new_effective_timestamp",
          LAG("past_effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_past_effective_timestamp",
          LAG("new_col_int", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_new_col_int",
          LAG("past_col_int", 1) OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "lag_past_col_int"
        FROM (
          SELECT
            "col_text",
            "new_effective_timestamp",
            "new_col_int",
            LAG("new_effective_timestamp") OVER (PARTITION BY "col_text" ORDER BY "new_effective_timestamp") AS "past_effective_timestamp",
            "past_col_int"
          FROM (
            SELECT
              "col_text",
              "effective_timestamp" AS "new_effective_timestamp",
              "col_int" AS "new_col_int",
              LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS "past_col_int"
            FROM (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "is_active" AS "is_active",
                "col_text" AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                "effective_timestamp" AS "effective_timestamp",
                "end_timestamp" AS "end_timestamp",
                "created_at" AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."scd_table"
            )
            QUALIFY
              "col_int" IS DISTINCT FROM LAG("col_int") OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp")
          )
        )
        LIMIT 10
        """
    ).strip()
    assert snowflake_change_view.preview_sql() == expected

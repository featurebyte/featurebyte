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
from featurebyte.api.entity import Entity
from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.critical_data_info import MissingValueImputation
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.query_graph.node.nested import ColumnCleaningOperation
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import check_sdk_code_generation


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


@pytest.fixture(name="snowflake_scd_data_with_imputation")
def snowflake_scd_data_with_imputation_fixture(snowflake_scd_data_with_entity):
    """
    Fixture for a SCDTableData with imputation
    """
    snowflake_scd_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value=-1)]
    )
    return snowflake_scd_data_with_entity


def test_auto_view_mode(snowflake_scd_data_with_imputation):
    """
    Test auto view mode
    """
    # create view
    snowflake_change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data_with_imputation, "col_int"
    )

    # check view graph metadata
    metadata = snowflake_change_view.node.parameters.metadata
    assert snowflake_scd_data_with_imputation.record_creation_date_column is None
    assert metadata.view_mode == "auto"
    assert metadata.drop_column_names == []
    assert metadata.column_cleaning_operations == [
        {
            "column_name": "col_int",
            "cleaning_operations": [{"imputed_value": -1, "type": "missing"}],
        }
    ]
    assert metadata.data_id == snowflake_scd_data_with_imputation.id

    # check that cleaning graph is created
    nested_graph = snowflake_change_view.node.parameters.graph
    cleaning_graph_node = nested_graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.type == "cleaning"


def test_manual_view_mode(snowflake_scd_data_with_imputation):
    """
    Test manual view mode
    """
    # create view
    snowflake_change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data_with_imputation, "col_int", view_mode="manual"
    )

    # check view graph metadata
    metadata = snowflake_change_view.node.parameters.metadata
    assert metadata.view_mode == "manual"
    assert metadata.drop_column_names == []
    assert metadata.column_cleaning_operations == []
    assert metadata.data_id == snowflake_scd_data_with_imputation.id

    # check that there is no cleaning graph
    nested_graph = snowflake_change_view.node.parameters.graph
    assert all(not node_name.startswith("graph") for node_name in nested_graph.nodes_map.keys())


def test_view_mode__auto_manual_equality_check(snowflake_scd_data_with_imputation):
    """
    Test view mode (create a view in auto mode, then create another equivalent view in manual mode).
    The equality is checked by comparing the view graphs. By using this relationship, we can
    reconstruct the view graph in manual mode from the view graph in auto mode.
    """
    # create view using auto mode
    view_auto = ChangeView.from_slowly_changing_data(snowflake_scd_data_with_imputation, "col_int")

    # create another equivalent view using manual mode
    snowflake_scd_data_with_imputation["col_int"].update_critical_data_info(cleaning_operations=[])
    drop_column_names = view_auto.node.parameters.metadata.drop_column_names
    view_manual = ChangeView.from_slowly_changing_data(
        snowflake_scd_data_with_imputation,
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


def change_view_test_helper(snowflake_scd_data, change_view):
    """
    Helper method to do some asserts
    """
    assert len(change_view.columns_info) == 5
    assert change_view.timestamp_column == "new_effective_timestamp"
    assert change_view.natural_key_column == snowflake_scd_data.natural_key_column
    assert change_view.columns == [
        "col_text",
        "new_effective_timestamp",
        "past_effective_timestamp",
        "new_col_int",
        "past_col_int",
    ]


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


def test_from_slowly_changing_data__check_entity_id(snowflake_scd_data):
    """
    Test from_slowly_changing_data - entity_id from the SCD data is correctly set
    """
    entity_key = Entity(name="key_column", serving_names=["key_column"])
    entity_eff_ts = Entity(name="eff_timestamp", serving_names=["eff_timestamp"])
    entity_change = Entity(name="change", serving_names=["change"])
    for entity in [entity_key, entity_eff_ts, entity_change]:
        entity.save()

    snowflake_scd_data[snowflake_scd_data.natural_key_column].as_entity("key_column")
    snowflake_scd_data[snowflake_scd_data.effective_timestamp_column].as_entity("eff_timestamp")
    snowflake_scd_data.col_int.as_entity("change")

    # create change view
    change_view = ChangeView.from_slowly_changing_data(snowflake_scd_data, "col_int")
    assert change_view.dict()["columns_info"] == [
        {
            "critical_data_info": None,
            "dtype": "VARCHAR",
            "entity_id": entity_key.id,
            "name": "col_text",
            "semantic_id": None,
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
            "semantic_id": None,
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
                "effective_timestamp" AS "new_effective_timestamp"
              FROM "sf_database"."sf_schema"."scd_table"
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


def test_from_slowly_changing_data__keep_record_creation_date_column(snowflake_scd_data):
    """
    Test create ChangeView using record creation date column as track changes column
    """
    snowflake_scd_data.update_record_creation_date_column("created_at")
    change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data, track_changes_column=snowflake_scd_data.record_creation_date_column
    )
    expected_sql = textwrap.dedent(
        """
        SELECT
          "col_text" AS "col_text",
          CAST("effective_timestamp" AS STRING) AS "new_effective_timestamp",
          CAST(LAG("effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS STRING) AS "past_effective_timestamp",
          CAST("created_at" AS STRING) AS "new_created_at",
          CAST(LAG("created_at", 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS STRING) AS "past_created_at"
        FROM "sf_database"."sf_schema"."scd_table"
        LIMIT 10
        """
    ).strip()
    assert change_view.preview_sql() == expected_sql
    assert change_view.node.type == NodeType.GRAPH
    assert change_view.node.parameters.graph.edges[:1] == [
        {"source": "proxy_input_1", "target": "project_1"},  # no cleaning operation
    ]

    # check the case when the data has cleaning operations
    # cleaned data should be used to generate the change view
    snowflake_scd_data["created_at"].update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="2020-01-01")]
    )
    change_view = ChangeView.from_slowly_changing_data(
        snowflake_scd_data, track_changes_column=snowflake_scd_data.record_creation_date_column
    )
    expected_sql = textwrap.dedent(
        """
        SELECT
          "col_text" AS "col_text",
          CAST("effective_timestamp" AS STRING) AS "new_effective_timestamp",
          CAST(LAG("effective_timestamp", 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS STRING) AS "past_effective_timestamp",
          CAST(CASE WHEN "created_at" IS NULL THEN '2020-01-01' ELSE "created_at" END AS STRING) AS "new_created_at",
          CAST(LAG(CASE WHEN "created_at" IS NULL THEN '2020-01-01' ELSE "created_at" END, 1) OVER (PARTITION BY "col_text" ORDER BY "effective_timestamp") AS STRING) AS "past_created_at"
        FROM "sf_database"."sf_schema"."scd_table"
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


def test_sdk_code_generation(saved_scd_data, update_fixtures):
    """Check SDK code generation"""
    to_use_saved_data = True
    change_view = ChangeView.from_slowly_changing_data(
        saved_scd_data,
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
        data_id=saved_scd_data.id,
    )

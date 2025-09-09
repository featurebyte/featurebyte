"""
Unit tests for aggregate_asat
"""

import pytest

from featurebyte.api.feature import Feature
from tests.util.helper import check_sdk_code_generation, get_node


def test_aggregate_asat__valid(
    snowflake_scd_view_with_entity, snowflake_scd_table, gender_entity_id
):
    """
    Test valid usage of aggregate_asat
    """
    feature = snowflake_scd_view_with_entity.groupby("col_boolean").aggregate_asat(
        value_column="col_float", method="sum", feature_name="asat_feature"
    )
    assert isinstance(feature, Feature)
    feature_dict = feature.model_dump()
    graph_dict = feature_dict["graph"]

    node_dict = get_node(graph_dict, feature_dict["node_name"])
    assert node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["asat_feature"]},
    }

    asat_node_dict = get_node(graph_dict, "aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": True,
            "current_flag_column": "is_active",
            "effective_timestamp_column": "effective_timestamp",
            "end_timestamp_column": "end_timestamp",
            "effective_timestamp_metadata": None,
            "end_timestamp_metadata": None,
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_feature",
            "natural_key_column": "col_text",
            "offset": None,
            "parent": "col_float",
            "serving_names": ["gender"],
            "snapshots_parameters": None,
            "value_by": None,
        },
        "type": "aggregate_as_at",
    }
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "aggregate_as_at_1"},
        {"source": "aggregate_as_at_1", "target": "project_1"},
    ]

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_scd_table.id: {
                "name": snowflake_scd_table.name,
                "record_creation_timestamp_column": snowflake_scd_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail during aggregate_asat
                "columns_info": scd_table_columns_info,
            }
        },
    )


def test_aggregate_asat__offset(snowflake_scd_view_with_entity, gender_entity_id):
    """
    Test offset parameter
    """
    feature = snowflake_scd_view_with_entity.groupby("col_boolean").aggregate_asat(
        value_column="col_float", method="sum", feature_name="asat_feature", offset="7d"
    )

    assert isinstance(feature, Feature)
    feature_dict = feature.model_dump()
    graph_dict = feature_dict["graph"]

    node_dict = get_node(graph_dict, feature_dict["node_name"])
    assert node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["asat_feature"]},
    }

    asat_node_dict = get_node(graph_dict, "aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": True,
            "current_flag_column": "is_active",
            "effective_timestamp_column": "effective_timestamp",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": "end_timestamp",
            "end_timestamp_metadata": None,
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_feature",
            "natural_key_column": "col_text",
            "offset": "7d",
            "parent": "col_float",
            "serving_names": ["gender"],
            "snapshots_parameters": None,
            "value_by": None,
        },
        "type": "aggregate_as_at",
    }


def test_aggregate_asat__not_backward(snowflake_scd_view_with_entity, gender_entity_id):
    """
    Test offset parameter
    """
    with pytest.warns(UserWarning, match="The backward parameter has no effect."):
        feature = snowflake_scd_view_with_entity.groupby("col_boolean").aggregate_asat(
            value_column="col_float",
            method="sum",
            feature_name="asat_feature",
            offset="7d",
            backward=False,
        )
    assert isinstance(feature, Feature)


def test_aggregate_asat_with_category(
    snowflake_scd_view_with_entity, snowflake_scd_table, gender_entity_id
):
    """Test aggregate_asat with category"""
    _ = snowflake_scd_table, gender_entity_id
    feature = snowflake_scd_view_with_entity.groupby(
        "col_boolean", category="col_text"
    ).aggregate_asat(
        value_column="col_float",
        method="sum",
        feature_name="asat_feature",
        offset="7d",
    )
    feature.save()

    # check that category is set correctly
    agg_info = feature.info()["metadata"]["aggregations"]
    assert agg_info == {
        "F0": {
            "aggregation_type": "aggregate_as_at",
            "category": "col_text",
            "column": "Input0",
            "filter": False,
            "function": "sum",
            "keys": ["col_boolean"],
            "name": "asat_feature",
            "offset": "7d",
            "window": None,
        }
    }


def test_aggregate_asat_snapshots_view(snowflake_snapshots_table_with_entity, another_entity):
    """
    Test aggregate_asat with snapshots view
    """
    view = snowflake_snapshots_table_with_entity.get_view()
    feature = view.groupby("col_binary", category="col_text").aggregate_asat(
        value_column="col_float",
        method="sum",
        feature_name="asat_feature",
        offset=7,
    )

    # Check node and parameters
    feature_dict = feature.model_dump()
    graph_dict = feature_dict["graph"]
    node_dict = get_node(graph_dict, feature_dict["node_name"])
    assert node_dict == {
        "name": "project_1",
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["asat_feature"]},
    }
    asat_node_dict = get_node(graph_dict, "aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": True,
            "current_flag_column": None,
            "effective_timestamp_column": "date",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": None,
            "end_timestamp_metadata": None,
            "entity_ids": [another_entity.id],
            "keys": ["col_binary"],
            "name": "asat_feature",
            "natural_key_column": None,
            "offset": None,
            "parent": "col_float",
            "serving_names": ["another_key"],
            "snapshots_parameters": {
                "feature_job_setting": None,
                "offset_size": 7,
                "snapshot_datetime_column": "date",
                "snapshot_datetime_metadata": {
                    "timestamp_schema": {
                        "format_string": "YYYY-MM-DD HH24:MI:SS",
                        "is_utc_time": None,
                        "timezone": "Etc/UTC",
                    },
                    "timestamp_tuple_schema": None,
                },
                "time_interval": {"unit": "DAY", "value": 1},
            },
            "value_by": "col_text",
        },
        "type": "aggregate_as_at",
    }

    # check feature can be saved
    feature.save()

    # check that category is set correctly
    agg_info = feature.info()["metadata"]["aggregations"]
    assert agg_info == {
        "F0": {
            "aggregation_type": "aggregate_as_at",
            "category": "col_text",
            "column": "Input0",
            "filter": False,
            "function": "sum",
            "keys": ["col_binary"],
            "name": "asat_feature",
            "offset": 7,
            "window": None,
        }
    }


def test_aggregate_asat_snapshots_view_invalid_key(snowflake_snapshots_table_with_entity):
    """
    Test aggregate_asat with snapshots view using an invalid groupby key
    """
    view = snowflake_snapshots_table_with_entity.get_view()
    with pytest.raises(ValueError) as exc_info:
        _ = view.groupby("col_int", category="col_text").aggregate_asat(
            value_column="col_float",
            method="sum",
            feature_name="asat_feature",
            offset="7d",
        )
    assert (
        str(exc_info.value)
        == "Snapshot ID column cannot be used as a groupby key in aggregate_asat"
    )

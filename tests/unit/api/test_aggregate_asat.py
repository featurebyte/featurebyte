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
    feature_dict = feature.dict()
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
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_feature",
            "natural_key_column": "col_text",
            "offset": None,
            "parent": "col_float",
            "serving_names": ["gender"],
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
    scd_table_columns_info = snowflake_scd_table.dict(by_alias=True)["columns_info"]
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
    feature_dict = feature.dict()
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
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_feature",
            "natural_key_column": "col_text",
            "offset": "7d",
            "parent": "col_float",
            "serving_names": ["gender"],
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

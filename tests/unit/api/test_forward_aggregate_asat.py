"""
Test forward_aggregate_asat
"""

import pytest

from featurebyte.api.target import Target
from tests.util.helper import check_sdk_code_generation, get_node


def test_forward_aggregate_asat__valid(
    snowflake_scd_view_with_entity, snowflake_scd_table, gender_entity_id
):
    """
    Test valid usage of forward_aggregate_asat
    """
    target = snowflake_scd_view_with_entity.groupby("col_boolean").forward_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="asat_target",
        fill_value=0.0,
    )
    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    target_aggregate_node_name = "project_1"
    node_dict = get_node(graph_dict, target_aggregate_node_name)
    assert node_dict == {
        "name": target_aggregate_node_name,
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["asat_target"]},
    }

    asat_node_dict = get_node(graph_dict, "forward_aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "forward_aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": None,
            "current_flag_column": "is_active",
            "effective_timestamp_column": "effective_timestamp",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": "end_timestamp",
            "end_timestamp_metadata": None,
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_target",
            "natural_key_column": "col_text",
            "offset": None,
            "parent": "col_float",
            "serving_names": ["gender"],
            "snapshots_parameters": None,
            "value_by": None,
        },
        "type": "forward_aggregate_as_at",
    }
    assert graph_dict["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "forward_aggregate_as_at_1"},
        {"source": "forward_aggregate_as_at_1", "target": "project_1"},
        {"source": "project_1", "target": "is_null_1"},
        {"source": "project_1", "target": "conditional_1"},
        {"source": "is_null_1", "target": "conditional_1"},
        {"source": "conditional_1", "target": "alias_1"},
    ]

    # check SDK code generation
    scd_table_columns_info = snowflake_scd_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        target,
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
    target = snowflake_scd_view_with_entity.groupby("col_boolean").forward_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="asat_target",
        offset="7d",
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    target_aggregate_node_name = "project_1"
    node_dict = get_node(graph_dict, target_aggregate_node_name)
    assert node_dict == {
        "name": target_aggregate_node_name,
        "type": "project",
        "output_type": "series",
        "parameters": {"columns": ["asat_target"]},
    }

    asat_node_dict = get_node(graph_dict, "forward_aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "forward_aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": None,
            "current_flag_column": "is_active",
            "effective_timestamp_column": "effective_timestamp",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": "end_timestamp",
            "end_timestamp_metadata": None,
            "entity_ids": [gender_entity_id],
            "keys": ["col_boolean"],
            "name": "asat_target",
            "natural_key_column": "col_text",
            "offset": "7d",
            "parent": "col_float",
            "serving_names": ["gender"],
            "snapshots_parameters": None,
            "value_by": None,
        },
        "type": "forward_aggregate_as_at",
    }


def test_forward_aggregate__fill_value(
    snowflake_scd_view_with_entity, snowflake_scd_table, gender_entity_id
):
    """
    Test forward_aggregate_asat with fill_value not set
    """
    with pytest.raises(ValueError) as exc:
        snowflake_scd_view_with_entity.groupby("col_boolean").forward_aggregate_asat(
            value_column="col_float",
            method="max",
            target_name="asat_target",
        )

    assert "fill_value is required for method max" in str(exc.value)

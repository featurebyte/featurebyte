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


def test_forward_aggregate_asat_snapshots_view(
    snowflake_snapshots_table_with_entity, another_entity
):
    """
    Test forward_aggregate_asat with snapshots view
    """
    view = snowflake_snapshots_table_with_entity.get_view()
    target = view.groupby("col_binary").forward_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="asat_target",
        offset=3,
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forward_aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "forward_aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": None,
            "current_flag_column": None,
            "effective_timestamp_column": "date",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": None,
            "end_timestamp_metadata": None,
            "entity_ids": [another_entity.id],
            "keys": ["col_binary"],
            "name": "asat_target",
            "natural_key_column": None,
            "offset": None,
            "parent": "col_float",
            "serving_names": ["another_key"],
            "snapshots_parameters": {
                "feature_job_setting": None,
                "offset_size": 3,
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
            "value_by": None,
        },
        "type": "forward_aggregate_as_at",
    }

    # check SDK code generation
    table_columns_info = snowflake_snapshots_table_with_entity.model_dump(by_alias=True)[
        "columns_info"
    ]
    check_sdk_code_generation(
        target,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_snapshots_table_with_entity.id: {
                "name": snowflake_snapshots_table_with_entity.name,
                "record_creation_timestamp_column": snowflake_snapshots_table_with_entity.record_creation_timestamp_column,
                "columns_info": table_columns_info,
            }
        },
    )


def test_forward_aggregate_asat_snapshots_view_no_offset(
    snowflake_snapshots_table_with_entity, another_entity
):
    """
    Test forward_aggregate_asat with snapshots view without offset
    """
    view = snowflake_snapshots_table_with_entity.get_view()
    target = view.groupby("col_binary").forward_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="asat_target_no_offset",
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forward_aggregate_as_at_1")
    assert asat_node_dict["parameters"]["snapshots_parameters"]["offset_size"] is None
    assert asat_node_dict["parameters"]["offset"] is None


def test_forward_aggregate_asat_time_series_view(
    snowflake_time_series_table_with_entity, cust_id_entity
):
    """
    Test forward_aggregate_asat with time series view
    """
    view = snowflake_time_series_table_with_entity.get_view()
    target = view.groupby("store_id").forward_aggregate_asat(
        value_column="col_float",
        method="sum",
        target_name="ts_asat_target",
        offset=7,
        fill_value=0.0,
    )

    assert isinstance(target, Target)
    target_dict = target.model_dump()
    graph_dict = target_dict["graph"]

    asat_node_dict = get_node(graph_dict, "forward_aggregate_as_at_1")
    assert asat_node_dict == {
        "name": "forward_aggregate_as_at_1",
        "output_type": "frame",
        "parameters": {
            "agg_func": "sum",
            "backward": None,
            "current_flag_column": None,
            "effective_timestamp_column": "date",
            "effective_timestamp_metadata": None,
            "end_timestamp_column": None,
            "end_timestamp_metadata": None,
            "entity_ids": [cust_id_entity.id],
            "keys": ["store_id"],
            "name": "ts_asat_target",
            "natural_key_column": None,
            "offset": None,
            "parent": "col_float",
            "serving_names": ["cust_id"],
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
            "value_by": None,
        },
        "type": "forward_aggregate_as_at",
    }

    # check SDK code generation
    table_columns_info = snowflake_time_series_table_with_entity.model_dump(by_alias=True)[
        "columns_info"
    ]
    check_sdk_code_generation(
        target,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_time_series_table_with_entity.id: {
                "name": snowflake_time_series_table_with_entity.name,
                "record_creation_timestamp_column": snowflake_time_series_table_with_entity.record_creation_timestamp_column,
                "columns_info": table_columns_info,
            }
        },
    )


def test_forward_aggregate_asat_snapshots_view_invalid_key(
    snowflake_snapshots_table_with_entity,
):
    """
    Test forward_aggregate_asat with snapshots view using series_id as groupby key
    """
    view = snowflake_snapshots_table_with_entity.get_view()
    with pytest.raises(ValueError) as exc_info:
        view.groupby("col_int").forward_aggregate_asat(
            value_column="col_float",
            method="sum",
            target_name="asat_target",
            fill_value=0.0,
        )
    assert "Series ID column cannot be used as a groupby key in aggregate_asat" in str(
        exc_info.value
    )


def test_forward_aggregate_asat_time_series_view_invalid_key(
    snowflake_time_series_table_with_entity,
):
    """
    Test forward_aggregate_asat with time series view using series_id as groupby key
    """
    view = snowflake_time_series_table_with_entity.get_view()
    with pytest.raises(ValueError) as exc_info:
        view.groupby("col_int").forward_aggregate_asat(
            value_column="col_float",
            method="sum",
            target_name="asat_target",
            fill_value=0.0,
        )
    assert "Series ID column cannot be used as a groupby key in aggregate_asat" in str(
        exc_info.value
    )

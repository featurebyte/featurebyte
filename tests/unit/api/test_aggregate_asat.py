"""
Unit tests for aggregate_asat
"""
import pytest

from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from tests.util.helper import check_sdk_code_generation, get_node


@pytest.fixture
def entity_col_int():
    entity = Entity(name="col_int_entity", serving_names=["col_int"])
    entity.save()
    return entity


@pytest.fixture
def scd_view_with_entity(snowflake_scd_table, entity_col_int):
    """
    Fixture for an SCDView with entity configured
    """
    Entity(name="col_text_entity", serving_names=["col_text"]).save()
    snowflake_scd_table["col_text"].as_entity("col_text_entity")
    snowflake_scd_table["col_int"].as_entity("col_int_entity")
    return snowflake_scd_table.get_view()


def test_aggregate_asat__valid(scd_view_with_entity, snowflake_scd_table, entity_col_int):
    """
    Test valid usage of aggregate_asat
    """
    feature = scd_view_with_entity.groupby("col_int").aggregate_asat(
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
        "type": "aggregate_as_at",
        "output_type": "frame",
        "parameters": {
            "effective_timestamp_column": "effective_timestamp",
            "natural_key_column": "col_text",
            "current_flag_column": "is_active",
            "end_timestamp_column": "end_timestamp",
            "keys": ["col_int"],
            "parent": "col_float",
            "agg_func": "sum",
            "value_by": None,
            "serving_names": ["col_int"],
            "entity_ids": [entity_col_int.id],
            "name": "asat_feature",
            "offset": None,
            "backward": True,
        },
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


def test_aggregate_asat__method_required(scd_view_with_entity):
    """
    Test method parameter is required
    """
    with pytest.raises(ValueError) as exc:
        scd_view_with_entity.groupby("col_int").aggregate_asat(
            value_column="col_float", feature_name="asat_feature"
        )
    assert str(exc.value) == "method is required"


def test_aggregate_asat__feature_name_required(scd_view_with_entity):
    """
    Test feature_name parameter is required
    """
    with pytest.raises(ValueError) as exc:
        scd_view_with_entity.groupby("col_int").aggregate_asat(
            value_column="col_float", feature_name="asat_feature"
        )
    assert str(exc.value) == "method is required"


def test_aggregate_asat__latest_not_supported(scd_view_with_entity):
    """
    Test using "latest" method is not supported
    """
    with pytest.raises(ValueError) as exc:
        scd_view_with_entity.groupby("col_int").aggregate_asat(
            value_column="col_float", method="latest", feature_name="asat_feature"
        )
    assert str(exc.value) == "latest aggregation method is not supported for aggregated_asat"


def test_aggregate_asat__groupby_key_cannot_be_natural_key(scd_view_with_entity):
    """
    Test using natural key as groupby key is not allowed
    """
    with pytest.raises(ValueError) as exc:
        scd_view_with_entity.groupby("col_text").aggregate_asat(
            value_column="col_float", method="sum", feature_name="asat_feature"
        )
    assert str(exc.value) == "Natural key column cannot be used as a groupby key in aggregate_asat"


def test_aggregate_asat__invalid_offset_string(scd_view_with_entity):
    """
    Test offset string is validated
    """
    with pytest.raises(ValueError) as exc:
        scd_view_with_entity.groupby("col_int").aggregate_asat(
            value_column="col_float", method="sum", feature_name="asat_feature", offset="yesterday"
        )
    assert "Failed to parse the offset parameter" in str(exc.value)


def test_aggregate_asat__offset_not_implemented(scd_view_with_entity):
    """
    Test offset support not yet implemented
    """
    with pytest.raises(NotImplementedError) as exc:
        scd_view_with_entity.groupby("col_int").aggregate_asat(
            value_column="col_float", method="sum", feature_name="asat_feature", offset="7d"
        )
    assert str(exc.value) == "offset support is not yet implemented"

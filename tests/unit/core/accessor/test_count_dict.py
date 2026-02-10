"""
Unit tests for core/accessor/count_dict.py
"""

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from tests.util.helper import check_sdk_code_generation, get_node


@pytest.mark.parametrize(
    "method, method_kwargs, expected_var_type, expected_parameters",
    [
        ("entropy", {}, DBVarType.FLOAT, {"transform_type": "entropy"}),
        ("most_frequent", {}, DBVarType.VARCHAR, {"transform_type": "most_frequent"}),
        (
            "key_with_highest_value",
            {},
            DBVarType.VARCHAR,
            {"transform_type": "key_with_highest_value"},
        ),
        (
            "key_with_lowest_value",
            {},
            DBVarType.VARCHAR,
            {"transform_type": "key_with_lowest_value"},
        ),
        (
            "unique_count",
            {},
            DBVarType.FLOAT,
            {"transform_type": "unique_count", "include_missing": True},
        ),
        (
            "unique_count",
            {"include_missing": False},
            DBVarType.FLOAT,
            {"transform_type": "unique_count", "include_missing": False},
        ),
        ("normalize", {}, DBVarType.OBJECT, {"transform_type": "normalize"}),
    ],
)
def test_transformation(
    snowflake_event_table,
    count_per_category_feature,
    method,
    method_kwargs,
    expected_var_type,
    expected_parameters,
):
    new_feature = getattr(count_per_category_feature.cd, method)(**method_kwargs)
    assert new_feature.node.output_type == NodeOutputType.SERIES
    assert new_feature.dtype == expected_var_type
    assert new_feature.node.type == NodeType.COUNT_DICT_TRANSFORM
    assert new_feature.node.parameters.model_dump(exclude_none=True) == expected_parameters
    assert new_feature.table_ids == count_per_category_feature.table_ids
    assert new_feature.entity_ids == count_per_category_feature.entity_ids

    # check SDK code generation
    event_table_columns_info = snowflake_event_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        new_feature,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_event_table.id: {
                "name": snowflake_event_table.name,
                "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail during GroupBy construction
                "columns_info": event_table_columns_info,
            }
        },
    )


def test_non_supported_feature_type(bool_feature):
    """Test count dict accessor on non-supported type"""
    with pytest.raises(AttributeError) as exc:
        bool_feature.cd.entropy()
    assert str(exc.value) == "Can only use .cd accessor with count per category features"


def test_cosine_similarity(
    snowflake_event_table, count_per_category_feature, count_per_category_feature_2h
):
    """
    Test cosine_similarity operation
    """
    result = count_per_category_feature.cd.cosine_similarity(count_per_category_feature_2h)
    result_dict = result.model_dump()
    assert result_dict["graph"]["edges"] == [
        {"source": "input_1", "target": "graph_1"},
        {"source": "graph_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
        {"source": "groupby_1", "target": "project_2"},
        {"source": "project_2", "target": "cosine_similarity_1"},
        {"source": "project_1", "target": "cosine_similarity_1"},
    ]
    cos_sim_node = get_node(result_dict["graph"], "cosine_similarity_1")
    assert cos_sim_node == {
        "name": "cosine_similarity_1",
        "type": NodeType.COSINE_SIMILARITY,
        "parameters": {},
        "output_type": "series",
    }

    # check SDK code generation
    event_table_columns_info = snowflake_event_table.model_dump(by_alias=True)["columns_info"]
    check_sdk_code_generation(
        result,
        to_use_saved_data=False,
        table_id_to_info={
            snowflake_event_table.id: {
                "name": snowflake_event_table.name,
                "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
                # since the table is not saved, we need to pass in the columns info
                # otherwise, entity id will be missing and code generation will fail during GroupBy construction
                "columns_info": event_table_columns_info,
            }
        },
    )


def test_cosine_similarity__other_not_dict_series(float_feature, count_per_category_feature):
    """
    Test cosine_similarity operation on non-dict feature (invalid)
    """
    with pytest.raises(TypeError) as exc:
        count_per_category_feature.cd.cosine_similarity(float_feature)
    assert (
        str(exc.value)
        == "cosine_similarity is only available for Feature of dictionary type; got FLOAT"
    )


def test_cosine_similarity__other_not_dict_scalar(count_per_category_feature):
    """
    Test cosine_similarity operation on non-dict scalar (invalid)
    """
    with pytest.raises(TypeError) as exc:
        count_per_category_feature.cd.cosine_similarity(123)
    assert str(exc.value) == "cosine_similarity is only available for Feature; got 123"


def test_get_value_from_dictionary__validation_fails(float_feature, count_per_category_feature):
    """
    Test validation will cause errors when features are not of the correct type.
    """
    with pytest.raises(AttributeError) as exc:
        float_feature.cd.get_value(float_feature)
    assert "Can only use .cd accessor with count per category features" in str(exc)


def test_get_value_from_dictionary__success(
    snowflake_event_table, count_per_category_feature, sum_per_category_feature
):
    """Test get_value method"""
    # count don't have parent column & sum has parent column,
    # use different aggregation methods to cover both cases
    event_table_columns_info = snowflake_event_table.model_dump(by_alias=True)["columns_info"]
    for per_cat_feat in [count_per_category_feature, sum_per_category_feature]:
        # check the count_dict has a proper dtype
        count_dict_op_struct = per_cat_feat.graph.extract_operation_structure(
            node=per_cat_feat.node, keep_all_source_columns=True
        )
        assert len(count_dict_op_struct.aggregations) == 1
        assert count_dict_op_struct.aggregations[0].dtype == "OBJECT"

        result = per_cat_feat.cd.get_value("key")
        result_dict = result.model_dump()
        if per_cat_feat.name == count_per_category_feature.name:
            expected_dtype = "INT"
        else:
            expected_dtype = "FLOAT"
        assert result.dtype == expected_dtype
        assert result_dict["graph"]["edges"] == [
            {"source": "input_1", "target": "graph_1"},
            {"source": "graph_1", "target": "groupby_1"},
            {"source": "groupby_1", "target": "project_1"},
            {"source": "project_1", "target": "get_value_1"},
        ]
        assert result_dict["graph"]["nodes"][4] == {
            "name": "get_value_1",
            "output_type": "series",
            "parameters": {"value": "key"},
            "type": "get_value",
        }

        # check SDK code generation
        check_sdk_code_generation(
            result,
            to_use_saved_data=False,
            table_id_to_info={
                snowflake_event_table.id: {
                    "name": snowflake_event_table.name,
                    "record_creation_timestamp_column": snowflake_event_table.record_creation_timestamp_column,
                    # since the table is not saved, we need to pass in the columns info
                    # otherwise, entity id will be missing and code generation will fail during GroupBy construction
                    "columns_info": event_table_columns_info,
                }
            },
        )


def test_get_rank_from_dictionary__validation_fails(float_feature, count_per_category_feature):
    """
    Test validation will cause errors when features are not of the correct type.
    """
    with pytest.raises(AttributeError) as exc:
        float_feature.cd.get_rank(float_feature)
    assert "Can only use .cd accessor with count per category features" in str(exc)


def test_get_relative_frequency_from_dictionary__validation_fails(
    float_feature, count_per_category_feature
):
    """
    Test validation will cause errors when features are not of the correct type.
    """
    with pytest.raises(AttributeError) as exc:
        float_feature.cd.get_relative_frequency(float_feature)
    assert "Can only use .cd accessor with count per category features" in str(exc)


@pytest.mark.parametrize(
    "method",
    ["get_value", "get_rank", "get_relative_frequency"],
)
@pytest.mark.parametrize(
    "key_type,expected_ok",
    [
        ("scalar", True),
        ("feature", True),
        ("request_column", True),
        ("series", False),
    ],
)
def test_key_type_validation(
    count_per_category_feature,
    scd_lookup_feature,
    varchar_series,
    request_column_point_in_time,
    method,
    key_type,
    expected_ok,
):
    func = getattr(count_per_category_feature.cd, method)
    key_type_mapping = {
        "scalar": "my_key",
        "feature": scd_lookup_feature,
        "request_column": request_column_point_in_time,
        "series": varchar_series,
    }
    key = key_type_mapping[key_type]
    if expected_ok:
        _ = func(key)
    else:
        with pytest.raises(TypeError) as exc_info:
            func(key)
        assert str(exc_info.value) == "Operation between Feature and Series is not supported"

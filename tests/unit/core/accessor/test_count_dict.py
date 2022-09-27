"""
Unit tests for core/accessor/count_dict.py
"""
import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType


@pytest.mark.parametrize(
    "method, method_kwargs, expected_var_type, expected_parameters",
    [
        ("entropy", {}, DBVarType.FLOAT, {"transform_type": "entropy"}),
        ("most_frequent", {}, DBVarType.VARCHAR, {"transform_type": "most_frequent"}),
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
    ],
)
def test_transformation(
    count_per_category_feature, method, method_kwargs, expected_var_type, expected_parameters
):
    new_feature = getattr(count_per_category_feature.cd, method)(**method_kwargs)
    assert new_feature.node.output_type == NodeOutputType.SERIES
    assert new_feature.dtype == expected_var_type
    assert new_feature.node.type == NodeType.COUNT_DICT_TRANSFORM
    assert new_feature.node.parameters.dict(exclude_none=True) == expected_parameters
    assert new_feature.event_data_ids == count_per_category_feature.event_data_ids
    assert new_feature.entity_ids == count_per_category_feature.entity_ids


def test_non_supported_feature_type(bool_feature):
    """Test count dict accessor on non-supported type"""
    with pytest.raises(AttributeError) as exc:
        bool_feature.cd.entropy()
    assert str(exc.value) == "Can only use .cd accessor with count per category features"


def test_cosine_similarity(count_per_category_feature, count_per_category_feature_2h):
    """
    Test cosine_similarity operation
    """
    result = count_per_category_feature.cd.cosine_similarity(count_per_category_feature_2h)
    pruned_graph = result.dict()["graph"]
    assert pruned_graph["edges"] == [
        {"source": "input_1", "target": "groupby_1"},
        {"source": "groupby_1", "target": "project_1"},
        {"source": "groupby_1", "target": "project_2"},
        {"source": "project_2", "target": "cosine_similarity_1"},
        {"source": "project_1", "target": "cosine_similarity_1"},
    ]
    cos_sim_node = next(
        node for node in pruned_graph["nodes"] if node["name"] == "cosine_similarity_1"
    )
    assert cos_sim_node == {
        "name": "cosine_similarity_1",
        "type": NodeType.COSINE_SIMILARITY,
        "parameters": {},
        "output_type": "series",
    }


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

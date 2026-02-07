"""
Tests for Feature list related models
"""

import pytest
from bson.objectid import ObjectId
from typeguard import TypeCheckError

from featurebyte import FeatureListStatus
from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureReadinessDistribution,
    FeatureReadinessTransition,
)
from featurebyte.models.feature_list_namespace import FeatureListNamespaceModel
from tests.util.helper import compare_pydantic_obj


@pytest.fixture(name="feature_list_model_dict")
def feature_list_model_dict_fixture():
    """Fixture for a FeatureList dict"""
    feature_ids = [ObjectId("631af7f5b02b7992313dd577"), ObjectId("631af7f5b02b7992313dd576")]
    return {
        "name": "my_feature_list",
        "feature_ids": feature_ids,
        "readiness_distribution": [{"readiness": "DRAFT", "count": 2}],
        "version": {"name": "V220710", "suffix": None},
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "feature_list_namespace_id": ObjectId(),
        "deployed": False,
        "online_enabled_feature_ids": [],
        "primary_entity_ids": [],
        "feature_clusters": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "relationships_info": None,
        "features_entity_lookup_info": None,
        "supported_serving_entity_ids": [],
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "aggregation_ids": [],
        "feast_enabled": False,
        "context_id": None,
    }


@pytest.fixture(name="feature_list_namespace_model_dict")
def feature_list_namespace_model_dict_fixture():
    """Fixture for a FeatureListNamespace dict"""
    feature_namespace_ids = [
        ObjectId("631af7f5b02b7992313dd579"),
        ObjectId("631af7f5b02b7992313dd578"),
    ]
    feature_list_ids = [ObjectId("631af7f5b02b7992313dd585"), ObjectId("631af7f5b02b7992313dd584")]
    return {
        "name": "my_feature_list",
        "feature_namespace_ids": feature_namespace_ids,
        "feature_list_ids": feature_list_ids,
        "deployed_feature_list_ids": [],
        "status": "DRAFT",
        "role": "outcome-predictors",
        "default_feature_list_id": feature_list_ids[0],
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "catalog_id": DEFAULT_CATALOG_ID,
        "block_modification_by": [],
        "description": None,
        "is_deleted": False,
        "context_id": None,
    }


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.model_validate(feature_list_model_dict)
    serialized_feature_list = feature_list.model_dump(exclude={"id": True}, by_alias=True)
    feature_list_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") and key != "feature_ids" else value
        for key, value in feature_list_model_dict.items()
    }
    feature_list_dict_sorted_ids["dtype_distribution"] = []
    feature_list_dict_sorted_ids["entity_ids"] = []
    feature_list_dict_sorted_ids["features_primary_entity_ids"] = []
    feature_list_dict_sorted_ids["table_ids"] = []
    feature_list_dict_sorted_ids["feature_clusters_path"] = None
    feature_list_dict_sorted_ids["store_info"] = None
    feature_list_dict_sorted_ids["features_metadata"] = []
    assert serialized_feature_list == feature_list_dict_sorted_ids

    feature_list_json = feature_list.model_dump_json(by_alias=True)
    loaded_feature_list = FeatureListModel.model_validate_json(feature_list_json)
    assert loaded_feature_list == feature_list

    # test derive production readiness fraction
    feature_list_model_dict["readiness_distribution"] = [
        {"readiness": "PRODUCTION_READY", "count": 2}
    ]
    updated_feature_list = FeatureListModel.model_validate(feature_list_model_dict)
    assert updated_feature_list.readiness_distribution.derive_production_ready_fraction() == 1.0

    # DEV-556: check older record conversion
    feature_list_model_dict["_id"] = updated_feature_list.id
    feature_list_model_dict["version"] = "V220710"
    loaded_old_feature_list = FeatureListModel.model_validate(feature_list_model_dict)
    compare_pydantic_obj(loaded_old_feature_list.version, {"name": "V220710", "suffix": None})
    assert loaded_old_feature_list == updated_feature_list

    # check that feature list store info for older record
    assert loaded_old_feature_list.feast_enabled is False


def test_feature_list_namespace_model(feature_list_namespace_model_dict):
    """Test feature list namespace model"""
    feature_list_namespace = FeatureListNamespaceModel.model_validate(
        feature_list_namespace_model_dict
    )
    serialized_feature_list_namespace = feature_list_namespace.model_dump(
        exclude={"id": True}, by_alias=True
    )
    feature_list_namespace_model_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_list_namespace_model_dict.items()
    }
    assert serialized_feature_list_namespace == feature_list_namespace_model_dict_sorted_ids

    feature_list_namespace_json = feature_list_namespace.model_dump_json(by_alias=True)
    loaded_feature_list_namespace = FeatureListNamespaceModel.model_validate_json(
        feature_list_namespace_json
    )
    assert loaded_feature_list_namespace == feature_list_namespace


def test_feature_list_status_ordering():
    """Test to cover feature list status ordering"""
    assert (
        FeatureListStatus.DEPLOYED
        > FeatureListStatus.TEMPLATE
        > FeatureListStatus.PUBLIC_DRAFT
        > FeatureListStatus.DRAFT
        > FeatureListStatus.DEPRECATED
    )
    assert FeatureListStatus.min() == FeatureListStatus.DEPRECATED
    assert FeatureListStatus.max() == FeatureListStatus.DEPLOYED


@pytest.mark.parametrize(
    "left_dist, right_dist, expected",
    [
        (
            [{"readiness": "PRODUCTION_READY", "count": 5}, {"readiness": "DRAFT", "count": 0}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            True,
        ),
        (
            [{"readiness": "PRODUCTION_READY", "count": 5}, {"readiness": "DRAFT", "count": 4}],
            [{"readiness": "PRODUCTION_READY", "count": 9}],
            False,
        ),
        (
            [{"readiness": "PRODUCTION_READY", "count": 5}, {"readiness": "DRAFT", "count": 4}],
            [{"readiness": "PRODUCTION_READY", "count": 8}],
            ValueError,
        ),
    ],
)
def test_feature_readiness_distribution_equality_check(left_dist, right_dist, expected):
    """Test feature readiness distribution - equality comparison"""
    feat_readiness_dist1 = FeatureReadinessDistribution(left_dist)
    feat_readiness_dist2 = FeatureReadinessDistribution(right_dist)
    if isinstance(expected, bool):
        assert (feat_readiness_dist1 == feat_readiness_dist2) is expected
    elif issubclass(expected, Exception):
        with pytest.raises(expected) as exc:
            _ = feat_readiness_dist1 == feat_readiness_dist2
        err_msg = (
            "Invalid comparison between two feature readiness distributions with different sums."
        )
        assert err_msg in str(exc.value)


def test_feature_readiness_distribution__equality_invalid_type():
    """Test feature readiness distribution - equality comparison (invalid other type)"""
    feat_readiness_dist = FeatureReadinessDistribution([{"readiness": "DRAFT", "count": 10}])
    with pytest.raises(TypeCheckError) as exc:
        _ = feat_readiness_dist == [{"readiness": "DRAFT", "count": 10}]
    err_msg = 'argument "other" (list) is not an instance of featurebyte.models.feature_list.FeatureReadinessDistribution'
    assert err_msg in str(exc.value)


@pytest.mark.parametrize(
    "left_dist, right_dist, expected",
    [
        (
            # left_prod_ready_frac < right_prod_ready_frac
            [{"readiness": "PRODUCTION_READY", "count": 4}, {"readiness": "DRAFT", "count": 1}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            True,
        ),
        (
            # left_prod_ready_frac == right_prod_ready_frac, left has higher number of worse readiness features
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "DEPRECATED", "count": 2},
            ],
            [{"readiness": "PRODUCTION_READY", "count": 3}, {"readiness": "DRAFT", "count": 2}],
            True,
        ),
        (
            # left_prod_ready_frac > right_prod_ready_frac
            [
                {"readiness": "PRODUCTION_READY", "count": 4},
                {"readiness": "DEPRECATED", "count": 1},
            ],
            [{"readiness": "DEPRECATED", "count": 5}],
            False,
        ),
        (
            # left_prod_ready_frac == right_prod_ready_frac, right has higher number of worse readiness features
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "DRAFT", "count": 2},
                {"readiness": "DEPRECATED", "count": 3},
            ],
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "DRAFT", "count": 1},
                {"readiness": "DEPRECATED", "count": 4},
            ],
            False,
        ),
        (
            # left == right
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            False,
        ),
    ],
)
def test_readiness_distribution__less_than_check(left_dist, right_dist, expected):
    """Test feature readiness distribution - equality comparison"""
    feat_readiness_dist1 = FeatureReadinessDistribution(left_dist)
    feat_readiness_dist2 = FeatureReadinessDistribution(right_dist)
    assert (feat_readiness_dist1 < feat_readiness_dist2) is expected
    assert (feat_readiness_dist1 >= feat_readiness_dist2) is not expected


@pytest.mark.parametrize(
    "dist, expected",
    [
        ([], 0.0),
        (
            [
                {"readiness": "PRODUCTION_READY", "count": 1},
                {"readiness": "DEPRECATED", "count": 0},
            ],
            1.0,
        ),
        (
            [
                {"readiness": "PRODUCTION_READY", "count": 1},
                {"readiness": "DEPRECATED", "count": 1},
            ],
            0.5,
        ),
    ],
)
def test_readiness_distribution__derive_production_ready_fraction(dist, expected):
    """Test feature readiness distribution - derive production ready fraction"""
    feat_readiness_dist = FeatureReadinessDistribution(dist)
    assert feat_readiness_dist.derive_production_ready_fraction() == expected


def test_feature_readiness_distribution__worst_cast_worst_case():
    """Test feature readiness distribution - worst cast & total count"""
    feat_readiness_dist = FeatureReadinessDistribution([
        {"readiness": "DRAFT", "count": 2},
        {"readiness": "PRODUCTION_READY", "count": 5},
    ])
    worst_cast_readiness_dist = feat_readiness_dist.worst_case()
    assert feat_readiness_dist.total_count == 7
    assert isinstance(worst_cast_readiness_dist, FeatureReadinessDistribution)
    assert worst_cast_readiness_dist.model_dump() == [{"readiness": "DEPRECATED", "count": 7}]


@pytest.mark.parametrize(
    "from_readiness,to_readiness,expected",
    [
        ("PRODUCTION_READY", "DRAFT", {"DRAFT": 3, "PRODUCTION_READY": 4}),
        ("PRODUCTION_READY", "DEPRECATED", {"DEPRECATED": 1, "DRAFT": 2, "PRODUCTION_READY": 4}),
        ("DRAFT", "DEPRECATED", {"DEPRECATED": 1, "DRAFT": 1, "PRODUCTION_READY": 5}),
        ("DRAFT", "PRODUCTION_READY", {"DRAFT": 1, "PRODUCTION_READY": 6}),
    ],
)
def test_feature_readiness_distribution__transition(from_readiness, to_readiness, expected):
    """Test feature readiness distribution - readiness transition"""
    feat_readiness_dist = FeatureReadinessDistribution([
        {"readiness": "DRAFT", "count": 2},
        {"readiness": "PRODUCTION_READY", "count": 5},
    ])

    output = feat_readiness_dist.update_readiness(
        transition=FeatureReadinessTransition(
            from_readiness=from_readiness, to_readiness=to_readiness
        )
    )
    assert output == FeatureReadinessDistribution(
        [{"readiness": readiness, "count": count} for readiness, count in expected.items()],
    )

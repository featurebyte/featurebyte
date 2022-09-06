"""
Tests for Feature list related models
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
)


@pytest.fixture(name="feature_list_model_dict")
def feature_list_model_dict_fixture():
    """Fixture for a FeatureList dict"""
    return {
        "name": "my_feature_list",
        "feature_ids": [ObjectId(), ObjectId()],
        "readiness_distribution": [{"readiness": "DRAFT", "count": 2}],
        "readiness": "DRAFT",
        "status": None,
        "version": "V220710",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "entity_ids": [ObjectId()],
        "event_data_ids": [ObjectId()],
        "feature_list_namespace_id": ObjectId(),
    }


@pytest.fixture(name="feature_list_namespace_model_dict")
def feature_list_namespace_model_dict_fixture():
    """Fixture for a FeatureListNamespace dict"""
    feature_list_id = ObjectId()
    return {
        "name": "my_feature_list",
        "feature_list_ids": [feature_list_id],
        "readiness_distribution": [{"readiness": "DRAFT", "count": 2}],
        "readiness": "DRAFT",
        "default_feature_list_id": feature_list_id,
        "default_version_mode": "AUTO",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "entity_ids": [ObjectId()],
        "event_data_ids": [ObjectId()],
    }


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    feature_list_dict = feature_list.dict(exclude={"id": True})
    assert feature_list_dict == feature_list_model_dict
    feature_list_json = feature_list.json(by_alias=True)
    loaded_feature_list = FeatureListModel.parse_raw(feature_list_json)
    assert loaded_feature_list == feature_list

    # test derived readiness
    feature_list_model_dict["readiness_distribution"] = [
        {"readiness": "PRODUCTION_READY", "count": 2}
    ]
    updated_feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    assert updated_feature_list.readiness == "PRODUCTION_READY"


def test_feature_list_namespace_model(feature_list_namespace_model_dict):
    """Test feature list namespace model"""
    feature_list_namespace = FeatureListNamespaceModel.parse_obj(feature_list_namespace_model_dict)
    feature_list_namespace_dict = feature_list_namespace.dict(exclude={"id": True})
    assert feature_list_namespace_dict == feature_list_namespace_model_dict
    feature_list_namespace_json = feature_list_namespace.json(by_alias=True)
    loaded_feature_list_namespace = FeatureListNamespaceModel.parse_raw(feature_list_namespace_json)
    assert loaded_feature_list_namespace == feature_list_namespace

    # test derived readiness
    feature_list_namespace_model_dict["readiness_distribution"] = [
        {"readiness": "PRODUCTION_READY", "count": 2}
    ]
    updated_feature_list = FeatureListModel.parse_obj(feature_list_namespace_model_dict)
    assert updated_feature_list.readiness == "PRODUCTION_READY"


def test_feature_list_status_ordering():
    """Test to cover feature list status ordering"""
    assert (
        FeatureListStatus.PUBLISHED
        > FeatureListStatus.PUBLIC_DRAFT
        > FeatureListStatus.DRAFT
        > FeatureListStatus.DEPRECATED
    )
    assert FeatureListStatus.min() == FeatureListStatus.DEPRECATED
    assert FeatureListStatus.max() == FeatureListStatus.PUBLISHED


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
    """Test feature readiness distribution equality comparison"""
    feat_readiness_dist1 = FeatureReadinessDistribution(__root__=left_dist)
    feat_readiness_dist2 = FeatureReadinessDistribution(__root__=right_dist)
    if isinstance(expected, bool):
        assert (feat_readiness_dist1 == feat_readiness_dist2) is expected
    elif issubclass(expected, Exception):
        with pytest.raises(expected) as exc:
            _ = feat_readiness_dist1 == feat_readiness_dist2
        err_msg = (
            "Invalid comparison between two feature readiness distributions with different sums."
        )
        assert err_msg in str(exc.value)


def test_feature_readiness_distribution_equality_invalid_type():
    """Test feature readiness distribution equality comparison (invalid other type)"""
    feat_readiness_dist = FeatureReadinessDistribution(
        __root__=[{"readiness": "DRAFT", "count": 10}]
    )
    with pytest.raises(TypeError) as exc:
        _ = feat_readiness_dist == [{"readiness": "DRAFT", "count": 10}]
    err_msg = (
        'type of argument "other" must be featurebyte.models.feature_list.FeatureReadinessDistribution; '
        "got list instead"
    )
    assert err_msg in str(exc.value)


@pytest.mark.parametrize(
    "left_dist, right_dist, expected",
    [
        (
            [{"readiness": "PRODUCTION_READY", "count": 4}, {"readiness": "DRAFT", "count": 1}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            True,
        ),
        (
            [
                {"readiness": "PRODUCTION_READY", "count": 4},
                {"readiness": "QUARANTINE", "count": 1},
            ],
            [{"readiness": "DRAFT", "count": 5}],
            True,
        ),
        (
            [
                {"readiness": "PRODUCTION_READY", "count": 4},
                {"readiness": "DEPRECATED", "count": 1},
            ],
            [{"readiness": "QUARANTINE", "count": 5}],
            True,
        ),
        (
            [{"readiness": "DRAFT", "count": 5}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            True,
        ),
        (
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            False,
        ),
    ],
)
def test_readiness_distribution_less_than_check(left_dist, right_dist, expected):
    """Test feature readiness distribution equality comparison"""
    feat_readiness_dist1 = FeatureReadinessDistribution(__root__=left_dist)
    feat_readiness_dist2 = FeatureReadinessDistribution(__root__=right_dist)
    assert (feat_readiness_dist1 < feat_readiness_dist2) is expected
    assert (feat_readiness_dist1 >= feat_readiness_dist2) is not expected

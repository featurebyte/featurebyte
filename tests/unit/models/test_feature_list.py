"""
Tests for Feature list related models
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FeatureReadinessTransition,
)


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
        "feature_clusters": None,
        "catalog_id": DEFAULT_CATALOG_ID,
    }


@pytest.fixture(name="feature_list_namespace_model_dict")
def feature_list_namespace_model_dict_fixture():
    """Fixture for a FeatureListNamespace dict"""
    feature_namespace_ids = [
        ObjectId("631af7f5b02b7992313dd579"),
        ObjectId("631af7f5b02b7992313dd578"),
    ]
    feature_list_ids = [ObjectId("631af7f5b02b7992313dd585"), ObjectId("631af7f5b02b7992313dd584")]
    entity_ids = [ObjectId("631af7f5b02b7992313dd581"), ObjectId("631af7f5b02b7992313dd580")]
    table_ids = [ObjectId("631af7f5b02b7992313dd583"), ObjectId("631af7f5b02b7992313dd582")]
    return {
        "name": "my_feature_list",
        "feature_namespace_ids": feature_namespace_ids,
        "dtype_distribution": [{"dtype": "FLOAT", "count": 2}],
        "feature_list_ids": feature_list_ids,
        "deployed_feature_list_ids": [],
        "readiness_distribution": [{"readiness": "DRAFT", "count": 2}],
        "status": "DRAFT",
        "default_feature_list_id": feature_list_ids[0],
        "default_version_mode": "AUTO",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "entity_ids": entity_ids,
        "table_ids": table_ids,
        "catalog_id": DEFAULT_CATALOG_ID,
    }


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    serialized_feature_list = feature_list.dict(exclude={"id": True})
    feature_list_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_list_model_dict.items()
    }
    assert serialized_feature_list == feature_list_dict_sorted_ids

    feature_list_json = feature_list.json(by_alias=True)
    loaded_feature_list = FeatureListModel.parse_raw(feature_list_json)
    assert loaded_feature_list == feature_list

    # test derive production readiness fraction
    feature_list_model_dict["readiness_distribution"] = [
        {"readiness": "PRODUCTION_READY", "count": 2}
    ]
    updated_feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    assert updated_feature_list.readiness_distribution.derive_production_ready_fraction() == 1.0

    # DEV-556: check older record conversion
    feature_list_model_dict["_id"] = updated_feature_list.id
    feature_list_model_dict["version"] = "V220710"
    loaded_old_feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    assert loaded_old_feature_list.version == {"name": "V220710", "suffix": None}
    assert loaded_old_feature_list == updated_feature_list


def test_feature_list_namespace_model(feature_list_namespace_model_dict):
    """Test feature list namespace model"""
    feature_list_namespace = FeatureListNamespaceModel.parse_obj(feature_list_namespace_model_dict)
    serialized_feature_list_namespace = feature_list_namespace.dict(exclude={"id": True})
    feature_list_namespace_model_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_list_namespace_model_dict.items()
    }
    assert serialized_feature_list_namespace == feature_list_namespace_model_dict_sorted_ids

    feature_list_namespace_json = feature_list_namespace.json(by_alias=True)
    loaded_feature_list_namespace = FeatureListNamespaceModel.parse_raw(feature_list_namespace_json)
    assert loaded_feature_list_namespace == feature_list_namespace


def test_feature_list_status_ordering():
    """Test to cover feature list status ordering"""
    assert (
        FeatureListStatus.TEMPLATE
        > FeatureListStatus.PUBLIC_DRAFT
        > FeatureListStatus.DRAFT
        > FeatureListStatus.DEPRECATED
    )
    assert FeatureListStatus.min() == FeatureListStatus.DEPRECATED
    assert FeatureListStatus.max() == FeatureListStatus.TEMPLATE


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


def test_feature_readiness_distribution__equality_invalid_type():
    """Test feature readiness distribution - equality comparison (invalid other type)"""
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
            # left_prod_ready_frac < right_prod_ready_frac
            [{"readiness": "PRODUCTION_READY", "count": 4}, {"readiness": "DRAFT", "count": 1}],
            [{"readiness": "PRODUCTION_READY", "count": 5}],
            True,
        ),
        (
            # left_prod_ready_frac == right_prod_ready_frac, left has higher number of worse readiness features
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "QUARANTINE", "count": 2},
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
            [{"readiness": "QUARANTINE", "count": 5}],
            False,
        ),
        (
            # left_prod_ready_frac == right_prod_ready_frac, right has higher number of worse readiness features
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "DRAFT", "count": 2},
                {"readiness": "QUARANTINE", "count": 2},
                {"readiness": "DEPRECATED", "count": 1},
            ],
            [
                {"readiness": "PRODUCTION_READY", "count": 3},
                {"readiness": "DRAFT", "count": 1},
                {"readiness": "QUARANTINE", "count": 3},
                {"readiness": "DEPRECATED", "count": 1},
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
    feat_readiness_dist1 = FeatureReadinessDistribution(__root__=left_dist)
    feat_readiness_dist2 = FeatureReadinessDistribution(__root__=right_dist)
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
    feat_readiness_dist = FeatureReadinessDistribution(__root__=dist)
    assert feat_readiness_dist.derive_production_ready_fraction() == expected


def test_feature_readiness_distribution__worst_cast_worst_case():
    """Test feature readiness distribution - worst cast & total count"""
    feat_readiness_dist = FeatureReadinessDistribution(
        __root__=[
            {"readiness": "DRAFT", "count": 2},
            {"readiness": "PRODUCTION_READY", "count": 5},
        ]
    )
    worst_cast_readiness_dist = feat_readiness_dist.worst_case()
    assert feat_readiness_dist.total_count == 7
    assert isinstance(worst_cast_readiness_dist, FeatureReadinessDistribution)
    assert worst_cast_readiness_dist.__root__ == [{"readiness": "DEPRECATED", "count": 7}]


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
    feat_readiness_dist = FeatureReadinessDistribution(
        __root__=[
            {"readiness": "DRAFT", "count": 2},
            {"readiness": "PRODUCTION_READY", "count": 5},
        ]
    )

    output = feat_readiness_dist.update_readiness(
        transition=FeatureReadinessTransition(
            from_readiness=from_readiness, to_readiness=to_readiness
        )
    )
    assert output == FeatureReadinessDistribution(
        __root__=[
            {"readiness": readiness, "count": count} for readiness, count in expected.items()
        ],
    )

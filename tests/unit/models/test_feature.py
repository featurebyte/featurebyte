"""
Tests for Feature related models
"""
from datetime import datetime

import pytest

from featurebyte.api.entity import Entity
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureListModel,
    FeatureModel,
    FeatureNameSpace,
    FeatureReadiness,
)


@pytest.fixture(name="feature_list_model_dict")
def feature_list_model_dict_fixture():
    """Fixture for a FeatureList dict"""
    return {
        "name": "my_feature_list",
        "description": None,
        "features": [],
        "readiness": None,
        "status": None,
        "version": "",
        "created_at": None,
    }


@pytest.fixture(name="feature_name_space_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    return {
        "name": "some_feature_name",
        "description": None,
        "versions": [],
        "readiness": FeatureReadiness.DRAFT,
        "created_at": datetime.now(),
        "default_version": "some_version",
        "default_version_mode": DefaultVersionMode.MANUAL,
    }


def test_feature_model(snowflake_event_view, feature_model_dict, mock_get_persistent):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    _ = mock_get_persistent
    Entity.create(name="customer", serving_name="cust_id")
    snowflake_event_view.cust_id.as_entity("customer")
    feature_group = snowflake_event_view.groupby(by_keys="cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        blind_spot="10m",
        frequency="30m",
        time_modulo_frequency="5m",
        feature_names=["sum_30m"],
    )
    feature = feature_group["sum_30m"]
    assert feature.dict() == feature_model_dict
    feature_json = feature.json()
    feature_loaded = FeatureModel.parse_raw(feature_json)
    for key in feature_model_dict.keys():
        if not key in {"graph", "node", "lineage", "row_index_lineage"}:
            # feature_json uses pruned graph, feature uses global graph,
            # therefore the graph & node could be different
            assert getattr(feature, key) == getattr(feature_loaded, key)


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    feature_list_dict = feature_list.dict()
    assert feature_list_dict == feature_list_model_dict


def test_feature_name_space(feature_name_space_dict):
    """Test feature name space model"""
    feature_name_space = FeatureNameSpace.parse_obj(feature_name_space_dict)
    feat_name_space_dict = feature_name_space.dict()
    assert feat_name_space_dict == feature_name_space_dict

"""
Tests for Feature related models
"""
from datetime import datetime

import freezegun
import pytest
from bson.objectid import ObjectId

from featurebyte.api.entity import Entity
from featurebyte.models.feature import (
    FeatureListModel,
    FeatureListStatus,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureReadiness,
)


@pytest.fixture(name="feature_list_model_dict")
def feature_list_model_dict_fixture():
    """Fixture for a FeatureList dict"""
    return {
        "name": "my_feature_list",
        "feature_ids": [],
        "readiness": "DRAFT",
        "status": None,
        "version": "V220710",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
    }


@pytest.fixture(name="feature_name_space_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    version_id = ObjectId()
    return {
        "name": "some_feature_name",
        "version_ids": [version_id],
        "readiness": "DRAFT",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "default_version_id": version_id,
        "default_version_mode": "MANUAL",
        "user_id": None,
    }


@freezegun.freeze_time("2022-07-10")
def test_feature_model(snowflake_event_view, feature_model_dict):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    Entity(name="customer", serving_names=["cust_id"]).save()
    snowflake_event_view.cust_id.as_entity("customer")
    feature_group = snowflake_event_view.groupby(by_keys="cust_id").aggregate(
        value_column="col_float",
        method="sum",
        windows=["30m"],
        feature_names=["sum_30m"],
        feature_job_setting={
            "blind_spot": "10m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        },
    )
    feature = feature_group["sum_30m"]
    feature_model_dict.pop("event_data_ids")
    assert (
        feature.dict(
            exclude={
                "id": True,
                "event_data_ids": True,
                "tabular_source": True,
                "feature_namespace_id": True,
            }
        )
        == feature_model_dict
    )
    feature_json = feature.json(by_alias=True)
    loaded_feature = FeatureModel.parse_raw(feature_json)
    assert loaded_feature.id == feature.id
    for key in feature_model_dict.keys():
        if key not in {"graph", "node", "row_index_lineage"}:
            # feature_json uses pruned graph, feature uses global graph,
            # therefore the graph & node could be different
            assert getattr(feature, key) == getattr(loaded_feature, key)


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    feature_list_dict = feature_list.dict(exclude={"id": True})
    assert feature_list_dict == feature_list_model_dict
    feature_list_json = feature_list.json(by_alias=True)
    loaded_feature_list = FeatureListModel.parse_raw(feature_list_json)
    assert loaded_feature_list == feature_list


def test_feature_name_space(feature_name_space_dict):
    """Test feature name space model"""
    feature_name_space = FeatureNamespaceModel.parse_obj(feature_name_space_dict)
    feat_name_space_dict = feature_name_space.dict(exclude={"id": True})
    assert feat_name_space_dict == feature_name_space_dict
    loaded_feature_name_space = FeatureNamespaceModel.parse_raw(
        feature_name_space.json(by_alias=True)
    )
    assert loaded_feature_name_space == feature_name_space


def test_feature_readiness_ordering():
    """Test to cover feature readiness ordering"""
    assert (
        FeatureReadiness.PRODUCTION_READY
        > FeatureReadiness.DRAFT
        > FeatureReadiness.QUARANTINE
        > FeatureReadiness.DEPRECATED
    )
    assert FeatureReadiness.min() == FeatureReadiness.DEPRECATED
    assert FeatureReadiness.max() == FeatureReadiness.PRODUCTION_READY


def test_feature_list_status_ordering():
    """Test to cover feature list status ordering"""
    assert (
        FeatureListStatus.PUBLISHED
        > FeatureListStatus.DRAFT
        > FeatureListStatus.EXPERIMENTAL
        > FeatureListStatus.DEPRECATED
    )
    assert FeatureListStatus.min() == FeatureListStatus.DEPRECATED
    assert FeatureListStatus.max() == FeatureListStatus.PUBLISHED

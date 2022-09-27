"""
Tests for Feature related models
"""
import json
import os
from datetime import datetime

import freezegun
import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel, FeatureReadiness


@pytest.fixture(name="feature_name_space_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    feature_ids = [ObjectId("631b00277280fc9aa9522794"), ObjectId("631b00277280fc9aa9522793")]
    entity_ids = [ObjectId("631b00277280fc9aa9522790"), ObjectId("631b00277280fc9aa9522789")]
    event_data_ids = [ObjectId("631b00277280fc9aa9522792"), ObjectId("631b00277280fc9aa9522791")]
    return {
        "name": "some_feature_name",
        "dtype": "FLOAT",
        "feature_ids": feature_ids,
        "online_enabled_feature_ids": [],
        "readiness": "DRAFT",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "default_feature_id": feature_ids[0],
        "default_version_mode": "MANUAL",
        "entity_ids": entity_ids,
        "event_data_ids": event_data_ids,
        "user_id": None,
    }


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_feature(test_dir):
    """Fixture for a Feature dict"""
    feature_fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(feature_fixture_path) as file_handle:
        return json.load(file_handle)


@freezegun.freeze_time("2022-07-10")
def test_feature_model(feature_model_dict):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    feature = FeatureModel(**feature_model_dict)
    feature_json = feature.json(by_alias=True)
    loaded_feature = FeatureModel.parse_raw(feature_json)
    assert loaded_feature.id == feature.id
    assert loaded_feature.dict() == feature.dict()

    # DEV-556: check older record conversion
    feature_model_dict = feature.dict(by_alias=True)
    feature_model_dict["online_enabled"] = None
    loaded_old_feature = FeatureModel.parse_obj(feature_model_dict)
    assert loaded_old_feature.online_enabled is False
    assert loaded_old_feature == loaded_feature


def test_feature_name_space(feature_name_space_dict):
    """Test feature name space model"""
    feature_name_space = FeatureNamespaceModel.parse_obj(feature_name_space_dict)
    serialized_feature_name_space = feature_name_space.dict(exclude={"id": True})
    feature_name_space_dict_sorted_ids = {
        key: sorted(value) if key.endswith("_ids") else value
        for key, value in feature_name_space_dict.items()
    }
    assert serialized_feature_name_space == feature_name_space_dict_sorted_ids
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

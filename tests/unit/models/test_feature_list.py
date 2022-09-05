"""
Tests for Feature list related models
"""
import pytest

from featurebyte.models.feature_list import FeatureListModel, FeatureListStatus


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
        "entity_ids": [],
        "event_data_ids": [],
    }


def test_feature_list_model(feature_list_model_dict):
    """Test feature list model"""
    feature_list = FeatureListModel.parse_obj(feature_list_model_dict)
    feature_list_dict = feature_list.dict(exclude={"id": True})
    assert feature_list_dict == feature_list_model_dict
    feature_list_json = feature_list.json(by_alias=True)
    loaded_feature_list = FeatureListModel.parse_raw(feature_list_json)
    assert loaded_feature_list == feature_list


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

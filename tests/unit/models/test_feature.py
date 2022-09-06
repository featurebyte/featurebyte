"""
Tests for Feature related models
"""
from datetime import datetime

import freezegun
import pytest
from bson.objectid import ObjectId

from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel, FeatureReadiness


@pytest.fixture(name="feature_name_space_dict")
def feature_name_space_dict_fixture():
    """Fixture for a FixtureNameSpace dict"""
    feature_id = ObjectId()
    entity_ids = [ObjectId()]
    event_data_ids = [ObjectId()]
    return {
        "name": "some_feature_name",
        "dtype": "FLOAT",
        "feature_ids": [feature_id],
        "readiness": "DRAFT",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "default_feature_id": feature_id,
        "default_version_mode": "MANUAL",
        "entity_ids": entity_ids,
        "event_data_ids": event_data_ids,
        "user_id": None,
    }


@freezegun.freeze_time("2022-07-10")
def test_feature_model(snowflake_event_view_with_entity, feature_model_dict):
    """Test feature model serialize & deserialize"""
    # pylint: disable=duplicate-code
    feature_group = snowflake_event_view_with_entity.groupby(by_keys="cust_id").aggregate(
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
    feature_model_dict.pop("entity_ids")
    assert (
        feature.dict(
            exclude={
                "id": True,
                "event_data_ids": True,
                "entity_ids": True,
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

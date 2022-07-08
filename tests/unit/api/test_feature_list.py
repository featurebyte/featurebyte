"""
Tests for featurebyte.api.feature_list
"""
import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte.api.feature_list import Feature, FeatureList, FeatureListStatus
from featurebyte.models.feature import FeatureReadiness


@pytest.fixture(name="production_ready_feature")
def production_ready_feature_fixture(feature_group):
    """Fixture for a production ready feature"""
    feature_group["production_ready_feature"] = feature_group["sum_30m"]
    feature = feature_group["production_ready_feature"]
    feature.readiness = FeatureReadiness.PRODUCTION_READY
    feature.version = "V220401"
    return feature


@pytest.fixture(name="draft_feature")
def draft_feature_fixture(feature_group):
    """Fixture for a draft feature"""
    feature_group["draft_feature"] = 123
    feature = feature_group["draft_feature"]
    feature.readiness = FeatureReadiness.DRAFT
    feature.version = "V220402"
    return feature


@pytest.fixture(name="quarantine_feature")
def quarantine_feature_fixture(feature_group):
    """Fixture for a quarantined feature"""
    feature_group["quarantine_feature"] = 123
    feature = feature_group["quarantine_feature"]
    feature.readiness = FeatureReadiness.QUARANTINE
    feature.version = "V220403"
    return feature


@pytest.fixture(name="deprecated_feature")
def deprecated_feature_fixture(feature_group):
    """Fixture for a deprecated feature"""
    feature_group["deprecated_feature"] = 123
    feature = feature_group["deprecated_feature"]
    feature.readiness = FeatureReadiness.DEPRECATED
    feature.version = "V220404"
    return feature


def test_features_readiness__deprecated(
    production_ready_feature, quarantine_feature, draft_feature, deprecated_feature
):
    """Test features readiness should evaluate to deprecated"""
    assert (
        FeatureList.derive_features_readiness(
            [
                production_ready_feature,
                draft_feature,
                quarantine_feature,
                deprecated_feature,
            ]
        )
        == FeatureReadiness.DEPRECATED
    )


def test_features_readiness__quarantine(
    production_ready_feature,
    quarantine_feature,
    draft_feature,
):
    """Test features readiness should evaluate to quarantine"""
    assert (
        FeatureList.derive_features_readiness(
            [
                production_ready_feature,
                draft_feature,
                quarantine_feature,
            ]
        )
        == FeatureReadiness.QUARANTINE
    )


def test_features_readiness__draft(production_ready_feature, draft_feature):
    """Test features readiness should evaluate to draft"""
    assert (
        FeatureList.derive_features_readiness(
            [
                production_ready_feature,
                draft_feature,
            ]
        )
        == FeatureReadiness.DRAFT
    )


def test_features_readiness__production_ready(
    production_ready_feature,
):
    """Test features readiness should evaluate to production ready"""
    assert (
        FeatureList.derive_features_readiness(
            [
                production_ready_feature,
            ]
        )
        == FeatureReadiness.PRODUCTION_READY
    )


@freeze_time("2022-05-01")
def test_feature_list_creation__success(production_ready_feature, config):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList([production_ready_feature], name="my_feature_list")
    dataframe = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-04-01", "2022-04-01"],
            "CUST_ID": ["C1", "C2"],
        }
    )
    flist.get_historical_features(dataframe, credentials=config.credentials)
    assert flist.dict() == {
        "name": "my_feature_list",
        "description": None,
        "features": [("production_ready_feature", "V220401")],
        "readiness": FeatureReadiness.PRODUCTION_READY,
        "status": FeatureListStatus.DRAFT,
        "version": "my_feature_list.V220501",
        "created_at": None,
    }
    for obj in flist.feature_objects:
        assert isinstance(obj, Feature)


@freeze_time("2022-05-01")
def test_feature_list_creation__feature_and_group(production_ready_feature, feature_group):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList(
        [production_ready_feature, feature_group[["sum_30m", "sum_1d"]]],
        name="my_feature_list",
    )
    assert flist.dict() == {
        "created_at": None,
        "description": None,
        "version": "my_feature_list.V220501",
        "features": [
            ("production_ready_feature", "V220401"),
            ("sum_30m", None),
            ("sum_1d", None),
        ],
        "name": "my_feature_list",
        "readiness": None,
        "status": "DRAFT",
    }
    for obj in flist.feature_objects:
        assert isinstance(obj, Feature)


def test_feature_list_creation__not_a_list():
    """Test FeatureList must be created from a list"""
    with pytest.raises(ValueError) as exc_info:
        FeatureList("my_feature", name="my_feature_list")
    assert str(exc_info.value) == "Cannot create feature list using <class 'str'>; expected a list"


def test_feature_list_creation__invalid_item():
    """Test FeatureList creation list cannot have invalid types"""
    with pytest.raises(ValueError) as exc_info:
        FeatureList(["my_feature"], name="my_feature_list")
    assert str(exc_info.value) == (
        "Unexpected item type <class 'str'>; expected Feature or FeatureGroup"
    )

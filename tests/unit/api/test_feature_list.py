"""
Tests for featurebyte.api.feature_list
"""
import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup, FeatureList
from featurebyte.models.feature import FeatureListStatus, FeatureReadiness


@pytest.fixture(name="production_ready_feature")
def production_ready_feature_fixture(feature_group):
    """Fixture for a production ready feature"""
    feature = feature_group["sum_30m"] + 123
    feature.name = "production_ready_feature"
    feature.readiness = FeatureReadiness.PRODUCTION_READY
    feature.version = "V220401"
    feature_group["production_ready_feature"] = feature
    return feature


@pytest.fixture(name="draft_feature")
def draft_feature_fixture(feature_group):
    """Fixture for a draft feature"""
    feature = feature_group["production_ready_feature"] + 123
    feature.name = "draft_feature"
    feature.readiness = FeatureReadiness.DRAFT
    feature.version = "V220402"
    feature_group["draft_feature"] = feature
    return feature


@pytest.fixture(name="quarantine_feature")
def quarantine_feature_fixture(feature_group):
    """Fixture for a quarantined feature"""
    feature = feature_group["draft_feature"] + 123
    feature.name = "quarantine_feature"
    feature.readiness = FeatureReadiness.QUARANTINE
    feature.version = "V220403"
    feature_group["quarantine_feature"] = feature
    return feature


@pytest.fixture(name="deprecated_feature")
def deprecated_feature_fixture(feature_group):
    """Fixture for a deprecated feature"""
    feature = feature_group["quarantine_feature"] + 123
    feature.name = "deprecated_feature"
    feature.readiness = FeatureReadiness.DEPRECATED
    feature.version = "V220404"
    feature_group["deprecated_feature"] = feature
    return feature


def test_features_readiness__deprecated(
    production_ready_feature, draft_feature, quarantine_feature, deprecated_feature
):
    """Test features readiness should evaluate to deprecated"""
    assert (
        FeatureList(
            [
                production_ready_feature,
                draft_feature,
                quarantine_feature,
                deprecated_feature,
            ],
            name="feature_list_name",
        ).readiness
        == FeatureReadiness.DEPRECATED
    )


def test_features_readiness__quarantine(
    production_ready_feature, draft_feature, quarantine_feature
):
    """Test features readiness should evaluate to quarantine"""
    assert (
        FeatureList(
            [
                production_ready_feature,
                draft_feature,
                quarantine_feature,
            ],
            name="feature_list_name",
        ).readiness
        == FeatureReadiness.QUARANTINE
    )


def test_features_readiness__draft(production_ready_feature, draft_feature):
    """Test features readiness should evaluate to draft"""
    assert (
        FeatureList(
            [
                production_ready_feature,
                draft_feature,
            ],
            name="feature_list_name",
        ).readiness
        == FeatureReadiness.DRAFT
    )


def test_features_readiness__production_ready(production_ready_feature):
    """Test features readiness should evaluate to production ready"""
    assert (
        FeatureList(
            [
                production_ready_feature,
            ],
            name="feature_list_name",
        ).readiness
        == FeatureReadiness.PRODUCTION_READY
    )


@pytest.mark.usefixtures("mocked_tile_cache")
@freeze_time("2022-05-01")
def test_feature_list_creation__success(production_ready_feature, config):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList([production_ready_feature], name="my_feature_list")
    dataframe = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-04-01", "2022-04-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    flist.get_historical_features(dataframe, credentials=config.credentials)
    assert flist.dict(exclude={"id": True}) == {
        "name": "my_feature_list",
        "description": None,
        "features": [("production_ready_feature", "V220401")],
        "readiness": "PRODUCTION_READY",
        "status": "DRAFT",
        "version": "V220501",
        "created_at": None,
    }
    for obj in flist.feature_objects.values():
        assert isinstance(obj, Feature)


@freeze_time("2022-05-01")
def test_feature_list_creation__feature_and_group(production_ready_feature, feature_group):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList(
        [production_ready_feature, feature_group[["sum_30m", "sum_1d"]]],
        name="my_feature_list",
    )
    assert flist.dict(exclude={"id": True}) == {
        "created_at": None,
        "description": None,
        "version": "V220501",
        "features": [
            ("production_ready_feature", "V220401"),
            ("sum_30m", feature_group["sum_30m"].version),
            ("sum_1d", feature_group["sum_1d"].version),
        ],
        "name": "my_feature_list",
        "readiness": None,
        "status": "DRAFT",
    }
    for obj in flist.feature_objects.values():
        assert isinstance(obj, Feature)


def test_feature_list_creation__not_a_list():
    """Test FeatureList must be created from a list"""
    with pytest.raises(ValueError) as exc_info:
        FeatureList("my_feature", name="my_feature_list")
    assert "value is not a valid list (type=type_error.list)" in str(exc_info.value)


def test_feature_list_creation__invalid_item():
    """Test FeatureList creation list cannot have invalid types"""
    with pytest.raises(ValueError) as exc_info:
        FeatureList(["my_feature"], name="my_feature_list")
    assert "value is not a valid Feature type" in str(exc_info.value)


def test_base_feature_group(
    production_ready_feature, draft_feature, quarantine_feature, deprecated_feature
):
    """
    Test BaseFeatureGroup
    """
    feature_group = BaseFeatureGroup([production_ready_feature, draft_feature])
    new_feature_group = BaseFeatureGroup([feature_group, quarantine_feature, deprecated_feature])
    assert list(new_feature_group.feature_objects.keys()) == [
        "production_ready_feature",
        "draft_feature",
        "quarantine_feature",
        "deprecated_feature",
    ]
    assert dict(new_feature_group.feature_objects) == {
        "production_ready_feature": production_ready_feature,
        "draft_feature": draft_feature,
        "quarantine_feature": quarantine_feature,
        "deprecated_feature": deprecated_feature,
    }


def test_base_feature_group__feature_uniqueness_validation(production_ready_feature, draft_feature):
    """
    Test BaseFeatureGroup feature uniqueness validation logic
    """
    no_name_feature = production_ready_feature + 1
    with pytest.raises(ValueError) as exc:
        BaseFeatureGroup([no_name_feature])
    expected_msg = f'Feature (feature.id: "{no_name_feature.id}") name must not be None!'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        BaseFeatureGroup([production_ready_feature, production_ready_feature])
    expected_msg = 'Duplicated feature name (feature.name: "production_ready_feature")!'
    assert expected_msg in str(exc.value)

    draft_feature.id = production_ready_feature.id
    with pytest.raises(ValueError) as exc:
        BaseFeatureGroup([production_ready_feature, draft_feature])
    expected_msg = f'Duplicated feature id (feature.id: "{production_ready_feature.id}")!'
    assert expected_msg in str(exc.value)

    feature_group = BaseFeatureGroup([production_ready_feature])
    with pytest.raises(ValueError) as exc:
        BaseFeatureGroup([production_ready_feature, feature_group])
    expected_msg = 'Duplicated feature name (feature.name: "production_ready_feature")!'
    assert expected_msg in str(exc.value)

    with pytest.raises(ValueError) as exc:
        BaseFeatureGroup([draft_feature, feature_group])
    expected_msg = f'Duplicated feature id (feature.id: "{production_ready_feature.id}")!'
    assert expected_msg in str(exc.value)


def test_base_feature_group__getitem__(production_ready_feature, draft_feature):
    """
    Test BaseFeatureGroup sub-setting columns
    """
    feature_group = FeatureGroup([production_ready_feature, draft_feature])
    feature_list = FeatureList([production_ready_feature, draft_feature], name="my_feature_list")
    feat1 = feature_group["production_ready_feature"]
    feat2 = feature_list["production_ready_feature"]
    assert isinstance(feat1, Feature) and isinstance(feat2, Feature)
    assert feat1 == feat2 == production_ready_feature
    feat_group1 = feature_group[["production_ready_feature"]]
    feat_group2 = feature_list[["production_ready_feature"]]
    assert feat_group1 == feat_group2
    assert isinstance(feat_group1, FeatureGroup)
    assert feat_group1.feature_objects["production_ready_feature"] == production_ready_feature


def test_base_feature_group__drop(production_ready_feature, draft_feature, quarantine_feature):
    """
    Test BaseFeatureGroup dropping columns
    """
    feature_group = FeatureGroup([production_ready_feature, draft_feature, quarantine_feature])
    feature_list = FeatureList(
        [production_ready_feature, draft_feature, quarantine_feature], name="my_feature_list"
    )
    feat_group1 = feature_group.drop(["production_ready_feature"])
    feat_group2 = feature_list.drop(["production_ready_feature"])
    assert feat_group1 == feat_group2
    assert isinstance(feat_group1, FeatureGroup)
    assert feat_group1.feature_names == ["draft_feature", "quarantine_feature"]


@freeze_time("2022-07-20")
def test_feature_list__construction(production_ready_feature, draft_feature):
    """
    Test FeatureList creation
    """
    feature_list = FeatureList([production_ready_feature, draft_feature], name="my_feature_list")
    assert feature_list.readiness == FeatureReadiness.DRAFT
    assert feature_list.features == [
        ("production_ready_feature", "V220401"),
        ("draft_feature", "V220402"),
    ]
    assert feature_list.feature_names == ["production_ready_feature", "draft_feature"]
    assert feature_list.status == FeatureListStatus.DRAFT
    assert feature_list.version == "V220720"
    assert list(feature_list.feature_objects.keys()) == [
        "production_ready_feature",
        "draft_feature",
    ]
    assert dict(feature_list.feature_objects) == {
        "production_ready_feature": production_ready_feature,
        "draft_feature": draft_feature,
    }

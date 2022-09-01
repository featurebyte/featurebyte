"""
Tests for featurebyte.api.feature_list
"""
from unittest.mock import patch

import pandas as pd
import pytest
from freezegun import freeze_time

from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import BaseFeatureGroup, FeatureGroup, FeatureList
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature import FeatureListStatus, FeatureReadiness
from featurebyte.query_graph.enum import NodeType


@pytest.fixture(name="production_ready_feature")
def production_ready_feature_fixture(feature_group):
    """Fixture for a production ready feature"""
    feature = feature_group["sum_30m"] + 123
    feature.name = "production_ready_feature"
    assert feature.parent is None
    feature.__dict__["readiness"] = FeatureReadiness.PRODUCTION_READY
    feature.__dict__["version"] = "V220401"
    feature_group["production_ready_feature"] = feature
    return feature


@pytest.fixture(name="draft_feature")
def draft_feature_fixture(feature_group):
    """Fixture for a draft feature"""
    feature = feature_group["production_ready_feature"] + 123
    feature.name = "draft_feature"
    feature.__dict__["readiness"] = FeatureReadiness.DRAFT
    feature.__dict__["version"] = "V220402"
    feature_group["draft_feature"] = feature
    return feature


@pytest.fixture(name="quarantine_feature")
def quarantine_feature_fixture(feature_group):
    """Fixture for a quarantined feature"""
    feature = feature_group["draft_feature"] + 123
    feature.name = "quarantine_feature"
    feature.__dict__["readiness"] = FeatureReadiness.QUARANTINE
    feature.__dict__["version"] = "V220403"
    feature_group["quarantine_feature"] = feature
    return feature


@pytest.fixture(name="deprecated_feature")
def deprecated_feature_fixture(feature_group):
    """Fixture for a deprecated feature"""
    feature = feature_group["quarantine_feature"] + 123
    feature.name = "deprecated_feature"
    feature.__dict__["readiness"] = FeatureReadiness.DEPRECATED
    feature.__dict__["version"] = "V220404"
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
def test_feature_list_creation__success(production_ready_feature, config, mocked_tile_cache):
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
        "feature_ids": [production_ready_feature.id],
        "readiness": "PRODUCTION_READY",
        "status": "DRAFT",
        "version": "V220501",
        "created_at": None,
        "updated_at": None,
        "user_id": None,
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
        "updated_at": None,
        "user_id": None,
        "version": "V220501",
        "feature_ids": [
            production_ready_feature.id,
            feature_group["sum_30m"].id,
            feature_group["sum_1d"].id,
        ],
        "name": "my_feature_list",
        "readiness": None,
        "status": "DRAFT",
    }
    for obj in flist.feature_objects.values():
        assert isinstance(obj, Feature)


def test_feature_list_creation__not_a_list():
    """Test FeatureList must be created from a list"""
    with pytest.raises(TypeError) as exc_info:
        FeatureList("my_feature", name="my_feature_list")
    assert 'type of argument "items" must be a list; got str instead' in str(exc_info.value)


def test_feature_list_creation__invalid_item():
    """Test FeatureList creation list cannot have invalid types"""
    with pytest.raises(TypeError) as exc_info:
        FeatureList(["my_feature"], name="my_feature_list")
    error_message = (
        'type of argument "items"[0] must be one of '
        "(featurebyte.api.feature.Feature, featurebyte.api.feature_list.BaseFeatureGroup); got str instead"
    )
    assert error_message in str(exc_info.value)


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

    # note: user should not modify the id this way for normal use case
    draft_feature.__dict__["id"] = production_ready_feature.id
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
    assert feat1.parent is feature_group
    assert feat2.parent is None
    assert isinstance(feat1, Feature) and isinstance(feat2, Feature)
    assert feat1 == feat2 == production_ready_feature
    feat_group1 = feature_group[["production_ready_feature"]]
    feat_group2 = feature_list[["production_ready_feature"]]
    assert feat_group1 == feat_group2
    assert isinstance(feat_group1, FeatureGroup)
    assert feat_group1.feature_objects["production_ready_feature"] == production_ready_feature


def test_feature_group__getitem__type_not_supported(production_ready_feature):
    """
    Test FeatureGroup.__getitem__ with not supported key type
    """
    feature_group = FeatureGroup([production_ready_feature])
    with pytest.raises(TypeError) as exc:
        _ = feature_group[True]
    expected_msg = 'type of argument "item" must be one of (str, List[str]); got bool instead'
    assert expected_msg in str(exc.value)


def test_feature_group__setitem__unnamed_feature(production_ready_feature, feature_group):
    """
    Test FeatureGroup.__setitem__ works for unnamed feature
    """
    feature = feature_group["sum_30m"] + 456
    feature_group = FeatureGroup([production_ready_feature])
    feature_group["sum_30m_plus_456"] = feature

    feature_object = feature_group.feature_objects["sum_30m_plus_456"]
    feature_node = feature_object.node
    assert feature_node.type == NodeType.ALIAS
    assert feature_node.parameters == {"name": "sum_30m_plus_456"}

    # check name of the feature in FeatureGroup is updated
    assert feature_object.name == "sum_30m_plus_456"
    assert feature.name is None


def test_feature_group__setitem__different_name(production_ready_feature, draft_feature):
    """
    Test FeatureGroup.__setitem__ for a feature with different name is not allowed
    """
    feature_group = FeatureGroup([production_ready_feature])
    with pytest.raises(ValueError) as exc_info:
        feature_group["new_name"] = draft_feature
    assert str(exc_info.value) == 'Feature "draft_feature" cannot be renamed to "new_name"'


def test_feature_group__setitem__empty_name(production_ready_feature):
    """
    Test FeatureGroup.__setitem__ for a feature with different name is not allowed
    """
    feature_group = FeatureGroup([production_ready_feature])
    new_feature = production_ready_feature + 123
    with pytest.raises(TypeError) as exc_info:
        feature_group[None] = new_feature
    assert str(exc_info.value) == 'type of argument "key" must be str; got NoneType instead'


def test_feature_group__preview_zero_feature():
    """
    Test FeatureGroup preview with zero feature
    """
    feature_group = FeatureGroup([])
    with pytest.raises(ValueError) as exc:
        feature_group.preview(point_in_time_and_serving_name={})
    expected_msg = "There is no feature in the FeatureGroup object."
    assert expected_msg in str(exc.value)


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
    assert feature_list.saved is False
    assert feature_list.readiness == FeatureReadiness.DRAFT
    assert feature_list.feature_ids == [production_ready_feature.id, draft_feature.id]
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


@pytest.fixture(name="mock_insert_feature_list_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch(
        "featurebyte.service.feature_list.FeatureListService._insert_feature_list_registry"
    ) as mock:
        yield mock


@pytest.fixture(name="saved_feature_list")
def saved_feature_list_fixture(
    snowflake_feature_store,
    snowflake_event_data,
    float_feature,
    mock_insert_feature_registry,
    mock_insert_feature_list_registry,
):
    """
    Saved feature list fixture
    """
    _ = mock_insert_feature_list_registry, mock_insert_feature_registry
    snowflake_feature_store.save()
    snowflake_event_data.save()
    assert float_feature.tabular_source.feature_store_id == snowflake_feature_store.id
    feature_list = FeatureList([float_feature], name="my_feature_list")
    assert feature_list.saved is False
    feature_list_id_before = feature_list.id
    feature_list.save()
    assert feature_list.saved is True
    assert feature_list.id == feature_list_id_before
    assert feature_list.readiness == FeatureReadiness.DRAFT
    assert feature_list.name == "my_feature_list"
    return feature_list


def test_info(saved_feature_list):
    """
    Test info
    """
    verbose_info = saved_feature_list.info(verbose=True)
    non_verbose_info = saved_feature_list.info(verbose=False)
    expected_info = {"name": "my_feature_list", "readiness": "DRAFT", "status": "DRAFT"}
    expected_feature = {
        "is_default": None,
        "name": "sum_1d",
        "online_enabled": None,
        "readiness": "DRAFT",
        "dtype": "FLOAT",
    }
    assert non_verbose_info.items() > expected_info.items()
    assert verbose_info.items() > expected_info.items()
    assert verbose_info["features"] == non_verbose_info["features"]
    assert len(verbose_info["features"]) == 1
    assert verbose_info["features"][0].items() > expected_feature.items()


def test_get_feature_list(saved_feature_list):
    """
    Test get feature list using feature list name
    """
    loaded_feature_list_by_name = FeatureList.get(name=saved_feature_list.name)
    assert loaded_feature_list_by_name.dict() == saved_feature_list.dict()
    assert loaded_feature_list_by_name == saved_feature_list
    assert loaded_feature_list_by_name.feature_objects == saved_feature_list.feature_objects
    assert loaded_feature_list_by_name.items == saved_feature_list.items
    assert loaded_feature_list_by_name.saved is True

    loaded_feature_list_by_id = FeatureList.get_by_id(saved_feature_list.id)
    assert loaded_feature_list_by_id.dict() == saved_feature_list.dict()
    assert loaded_feature_list_by_id == saved_feature_list
    assert loaded_feature_list_by_id.feature_objects == saved_feature_list.feature_objects
    assert loaded_feature_list_by_id.items == saved_feature_list.items
    assert loaded_feature_list_by_id.saved is True

    # check unexpected exception in get
    with pytest.raises(RecordRetrievalException) as exc:
        FeatureList.get(name="random_name")
    expected_msg = (
        'FeatureList (name: "random_name") not found. Please save the FeatureList object first.'
    )
    assert expected_msg in str(exc.value)

    # check audit log
    audit_history = saved_feature_list.audit()
    expected_pagination_info = {"page": 1, "page_size": 10, "total": 1}
    assert audit_history.items() > expected_pagination_info.items()
    history_data = audit_history["data"]
    assert (
        history_data[0].items()
        > {
            "name": 'insert: "my_feature_list"',
            "action_type": "INSERT",
            "previous_values": {},
        }.items()
    )
    assert (
        history_data[0]["current_values"].items()
        > {
            "name": "my_feature_list",
            "status": "DRAFT",
            "readiness": "DRAFT",
            "updated_at": None,
            "user_id": None,
        }.items()
    )

    # check unexpected exception in audit
    with patch("featurebyte.api.api_object.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            saved_feature_list.audit()
    assert "Failed to list object audit log." in str(exc.value)


@patch("featurebyte.api.feature_list.alive_bar")
@patch("featurebyte.api.feature_list.is_notebook")
def test_pre_save_operations(mock_is_notebook, mock_alive_bar, saved_feature_list):
    """Test alive bar arguments in _pre_save_operations method"""
    mock_is_notebook.return_value = True
    saved_feature_list._pre_save_operations()
    assert mock_alive_bar.call_args.kwargs == {
        "total": 1,
        "title": "Saving Feature(s)",
        "force_tty": True,
    }

    mock_is_notebook.return_value = False
    saved_feature_list._pre_save_operations()
    assert mock_alive_bar.call_args.kwargs == {
        "total": 1,
        "title": "Saving Feature(s)",
        "dual_line": True,
    }

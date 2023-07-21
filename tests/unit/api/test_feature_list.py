"""
Tests for featurebyte.api.feature_list
"""
import textwrap
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from featurebyte import list_deployments
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_group import BaseFeatureGroup, FeatureGroup
from featurebyte.api.feature_list import FeatureList, FeatureListNamespace
from featurebyte.enum import InternalName
from featurebyte.exception import (
    RecordCreationException,
    RecordDeletionException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.feature_list import FeatureListStatus
from featurebyte.models.feature_namespace import DefaultVersionMode, FeatureReadiness
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


@pytest.fixture(name="draft_feature")
def draft_feature_fixture(feature_group):
    """Fixture for a draft feature"""
    feature = feature_group["production_ready_feature"] + 123
    feature.name = "draft_feature"
    feature.__dict__["version"] = "V220402"
    feature_group["draft_feature"] = feature
    return feature


@pytest.fixture(name="deprecated_feature")
def deprecated_feature_fixture(feature_group):
    """Fixture for a deprecated feature"""
    feature = feature_group["production_ready_feature"] + 123
    feature.name = "deprecated_feature"
    feature.__dict__["readiness"] = FeatureReadiness.DEPRECATED
    feature.__dict__["version"] = "V220404"
    feature_group["deprecated_feature"] = feature
    return feature


@pytest.fixture(name="single_feat_flist")
def single_feat_flist_fixture(production_ready_feature):
    """Feature list with a single feature"""
    flist = FeatureList([production_ready_feature], name="my_feature_list")
    return flist


@freeze_time("2022-05-01")
def test_feature_list_creation__success(
    production_ready_feature, single_feat_flist, mocked_compute_tiles_on_demand, catalog
):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList([production_ready_feature], name="my_feature_list")

    assert flist.dict(by_alias=True) == {
        "_id": flist.id,
        "name": "my_feature_list",
        "feature_ids": [production_ready_feature.id],
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "catalog_id": catalog.id,
        "block_modification_by": [],
        "description": None,
    }
    for obj in flist.feature_objects.values():
        assert isinstance(obj, Feature)

    with pytest.raises(RecordRetrievalException) as exc:
        _ = flist.status
    error_message = (
        f'FeatureList (id: "{flist.id}") not found. Please save the FeatureList object first.'
    )
    assert error_message in str(exc.value)


def test_feature_list__get_historical_features(single_feat_flist, mocked_compute_tiles_on_demand):
    """Test FeatureList can be created with valid inputs"""
    flist = single_feat_flist
    dataframe = pd.DataFrame(
        {
            "POINT_IN_TIME": ["2022-04-01", "2022-04-01"],
            "cust_id": ["C1", "C2"],
        }
    )
    mock_feature_table = Mock(name="TempFeatureTable")
    mock_object_id = ObjectId()
    with patch.object(
        FeatureList, "compute_historical_feature_table"
    ) as mock_compute_historical_feature_table:
        mock_compute_historical_feature_table.return_value = mock_feature_table
        with patch("featurebyte.api.feature_list.ObjectId", return_value=mock_object_id):
            flist.compute_historical_features(dataframe)

    # Check compute_historical_feature_table() is called correctly
    expected_dataframe = dataframe.copy()
    expected_dataframe[InternalName.ROW_INDEX] = [0, 1]
    _, kwargs = mock_compute_historical_feature_table.call_args
    assert expected_dataframe.equals(kwargs["observation_table"])
    assert (
        kwargs["historical_feature_table_name"]
        == f"__TEMPORARY_HISTORICAL_FEATURE_TABLE_{mock_object_id}"
    )
    assert kwargs["serving_names_mapping"] is None

    # Check temporary feature table is deleted
    mock_feature_table.delete.assert_called_once()


def test_feature_list_creation__feature_and_group(production_ready_feature, feature_group, catalog):
    """Test FeatureList can be created with valid inputs"""
    flist = FeatureList(
        [production_ready_feature, feature_group[["sum_30m", "sum_1d"]]],
        name="my_feature_list",
    )
    assert flist.dict(by_alias=True) == {
        "_id": flist.id,
        "created_at": None,
        "updated_at": None,
        "user_id": None,
        "feature_ids": [
            production_ready_feature.id,
            feature_group["sum_30m"].id,
            feature_group["sum_1d"].id,
        ],
        "name": "my_feature_list",
        "catalog_id": catalog.id,
        "block_modification_by": [],
        "description": None,
    }
    for obj in flist.feature_objects.values():
        assert isinstance(obj, Feature)


def test_feature_list_creation__not_a_list():
    """Test FeatureList must be created from a list"""
    with pytest.raises(TypeError) as exc_info:
        FeatureList("my_feature", name="my_feature_list")
    expected_error = (
        'type of argument "items"[0] must be one of (featurebyte.api.feature.Feature,'
        " BaseFeatureGroup); got str instead"
    )
    assert expected_error in str(exc_info.value)


def test_feature_list_creation__not_a_sequence():
    """Test FeatureList must be created from a list"""
    with pytest.raises(TypeError) as exc_info:
        FeatureList(123, name="my_feature_list")
    assert 'type of argument "items" must be a sequence; got int instead' == str(exc_info.value)


def test_feature_list_creation__invalid_item():
    """Test FeatureList creation list cannot have invalid types"""
    with pytest.raises(TypeError) as exc_info:
        FeatureList(["my_feature"], name="my_feature_list")
    error_message = (
        'type of argument "items"[0] must be one of '
        "(featurebyte.api.feature.Feature, BaseFeatureGroup); got str instead"
    )
    assert error_message in str(exc_info.value)


def test_base_feature_group(production_ready_feature, draft_feature, deprecated_feature):
    """
    Test BaseFeatureGroup
    """
    feature_group = BaseFeatureGroup([production_ready_feature, draft_feature])
    new_feature_group = BaseFeatureGroup([feature_group, deprecated_feature])
    assert list(new_feature_group.feature_objects.keys()) == [
        "production_ready_feature",
        "draft_feature",
        "deprecated_feature",
    ]
    assert dict(new_feature_group.feature_objects) == {
        "production_ready_feature": production_ready_feature,
        "draft_feature": draft_feature,
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
    assert (
        str(exc_info.value)
        == 'type of argument "key" must be one of (str, Tuple[featurebyte.api.feature.Feature, '
        "str]); got NoneType instead"
    )


def test_feature_group__setitem__with_series_not_allowed(production_ready_feature, saved_scd_table):
    """
    Test that FeatureGroup.__setitem__ for a series is not allowed.
    """
    scd_view = saved_scd_table.get_view()
    series = scd_view["col_int"]
    feature_group = FeatureGroup([production_ready_feature])
    with pytest.raises(TypeError) as exc:
        feature_group[series, "production_ready_feature"] = 900
    assert (
        'type of argument "key" must be one of (str, Tuple[featurebyte.api.feature.Feature, str]); got tuple instead'
        in str(exc)
    )


def test_feature_group__preview_zero_feature():
    """
    Test FeatureGroup preview with zero feature
    """
    feature_group = FeatureGroup([])
    with pytest.raises(ValueError) as exc:
        feature_group.preview(pd.DataFrame())
    expected_msg = "There is no feature in the FeatureGroup object."
    assert expected_msg in str(exc.value)


def test_base_feature_group__drop(production_ready_feature, draft_feature):
    """
    Test BaseFeatureGroup dropping columns
    """
    feature_group = FeatureGroup([production_ready_feature, draft_feature])
    feature_list = FeatureList([production_ready_feature, draft_feature], name="my_feature_list")
    feat_group1 = feature_group.drop(["production_ready_feature"])
    feat_group2 = feature_list.drop(["production_ready_feature"])
    assert feat_group1 == feat_group2
    assert isinstance(feat_group1, FeatureGroup)
    assert feat_group1.feature_names == ["draft_feature"]


def test_feature_list__construction(production_ready_feature, draft_feature):
    """
    Test FeatureList creation
    """
    feature_list = FeatureList([production_ready_feature, draft_feature], name="my_feature_list")
    assert feature_list.saved is False
    assert feature_list.feature_ids == [production_ready_feature.id, draft_feature.id]
    assert feature_list.feature_names == ["production_ready_feature", "draft_feature"]
    assert sorted(feature_list.feature_objects.keys()) == [
        "draft_feature",
        "production_ready_feature",
    ]
    assert dict(feature_list.feature_objects) == {
        "production_ready_feature": production_ready_feature,
        "draft_feature": draft_feature,
    }


@pytest.fixture(name="saved_feature_list")
def saved_feature_list_fixture(
    snowflake_event_table,
    float_feature,
):
    """
    Saved feature list fixture
    """
    assert float_feature.tabular_source.feature_store_id == snowflake_event_table.feature_store.id
    feature_list = FeatureList([float_feature], name="my_feature_list")
    assert feature_list.saved is False
    feature_list_id_before = feature_list.id
    feature_list.save()
    assert feature_list.saved is True
    assert feature_list.id == feature_list_id_before
    assert feature_list.name == "my_feature_list"
    assert feature_list.status == FeatureListStatus.DRAFT

    feature_list_namespace = feature_list.feature_list_namespace
    assert feature_list_namespace.name == "my_feature_list"
    assert feature_list_namespace.feature_list_ids == [feature_list.id]
    assert feature_list_namespace.default_feature_list_id == feature_list.id
    return feature_list


def test_deserialization(production_ready_feature, draft_feature):
    """
    Test deserialization
    """
    feature_group = FeatureGroup([production_ready_feature, draft_feature])
    feature_list = FeatureList([feature_group], name="my_feature_list")
    feature_list_dict = feature_list.dict(by_alias=True)
    expected_status = FeatureListStatus.TEMPLATE
    expected_version = {"name": "V220701", "suffix": None}
    feature_list_dict["status"] = expected_status
    feature_list_dict["version"] = expected_version

    with patch(
        "featurebyte.api.feature_group.iterate_api_object_using_paginated_routes"
    ) as mock_iterate:
        with patch("featurebyte.api.feature_store.FeatureStore._get_by_id") as mock_get_by_id:
            mock_get_by_id.return_value = production_ready_feature.feature_store
            mock_iterate.return_value = [
                production_ready_feature.dict(by_alias=True),
                draft_feature.dict(by_alias=True),
            ]
            loaded_feature_list = FeatureList(**feature_list_dict, items=[])

        # check that it is only called once
        assert mock_get_by_id.call_count == 1

    # check consistency between loaded feature list & original feature list
    assert loaded_feature_list.feature_ids == feature_list.feature_ids


def test_info(saved_feature_list):
    """
    Test info
    """
    info_dict = saved_feature_list.info()
    expected_info = {
        "name": "my_feature_list",
        "dtype_distribution": [{"dtype": "FLOAT", "count": 1}],
        "entities": [{"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}],
        "primary_entity": [
            {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}
        ],
        "tables": [{"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "catalog"}],
        "default_version_mode": "AUTO",
        "status": "DRAFT",
        "feature_count": 1,
        "version_count": 1,
        "production_ready_fraction": {"this": 0.0, "default": 0.0},
        "default_feature_fraction": {"this": 1.0, "default": 1.0},
        "deployed": False,
        "catalog_name": "catalog",
        "default_feature_list_id": str(saved_feature_list.id),
        "created_at": info_dict["created_at"],
        "version": info_dict["version"],
        "updated_at": info_dict["updated_at"],
        "versions_info": None,
        "description": None,
    }
    assert info_dict == expected_info, info_dict

    verbose_info_dict = saved_feature_list.info(verbose=True)
    assert verbose_info_dict == {
        **expected_info,
        "versions_info": [
            {
                "production_ready_fraction": 0.0,
                "readiness_distribution": [{"readiness": "DRAFT", "count": 1}],
                "created_at": verbose_info_dict["versions_info"][0]["created_at"],
                "version": verbose_info_dict["versions_info"][0]["version"],
            }
        ],
    }, verbose_info_dict


def test_get_feature_list(saved_feature_list, catalog):
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
        'FeatureListNamespace (name: "random_name") not found. '
        "Please save the FeatureListNamespace object first."
    )
    assert expected_msg in str(exc.value)

    # check audit log
    audit_history = saved_feature_list.audit()
    expected_audit_history = pd.DataFrame(
        [
            ("block_modification_by", []),
            ("catalog_id", str(catalog.id)),
            ("created_at", saved_feature_list.created_at.isoformat()),
            ("deployed", False),
            ("description", None),
            ("feature_clusters", audit_history.new_value.iloc[5]),
            ("feature_ids", [str(saved_feature_list.feature_ids[0])]),
            ("feature_list_namespace_id", str(saved_feature_list.feature_list_namespace.id)),
            ("name", "my_feature_list"),
            ("online_enabled_feature_ids", []),
            ("readiness_distribution", [{"readiness": "DRAFT", "count": 1}]),
            ("relationships_info", audit_history.new_value.iloc[11]),
            ("updated_at", None),
            ("user_id", None),
            ("version.name", saved_feature_list.version),
            ("version.suffix", None),
        ],
        columns=["field_name", "new_value"],
    )
    expected_audit_history["action_type"] = "INSERT"
    expected_audit_history["name"] = 'insert: "my_feature_list"'
    expected_audit_history["old_value"] = np.nan
    pd.testing.assert_frame_equal(
        audit_history[expected_audit_history.columns], expected_audit_history
    )

    # check unexpected exception in audit
    with patch("featurebyte.api.api_object_util.Configurations"):
        with pytest.raises(RecordRetrievalException) as exc:
            saved_feature_list.audit()
    assert f"Failed to list /feature_list/audit/{saved_feature_list.id}." in str(exc.value)


def test_list(saved_feature_list):
    """Test listing feature list"""
    feature_lists = FeatureList.list(include_id=True)
    saved_feature_list_namespace = FeatureListNamespace.get(saved_feature_list.name)
    assert_frame_equal(
        feature_lists,
        pd.DataFrame(
            {
                "id": [saved_feature_list.id],
                "name": [saved_feature_list_namespace.name],
                "num_feature": 1,
                "status": [saved_feature_list_namespace.status],
                "deployed": [saved_feature_list.deployed],
                "readiness_frac": 0.0,
                "online_frac": 0.0,
                "tables": [["sf_event_table"]],
                "entities": [["customer"]],
                "primary_entities": [["customer"]],
                "created_at": [saved_feature_list_namespace.created_at],
            }
        ),
    )


def test_list_versions(saved_feature_list):
    """Test listing feature list versions"""
    # save a few more feature list
    feature_group = FeatureGroup(items=[])
    feat = saved_feature_list["sum_1d"]
    feature_group[f"new_feat1"] = feat + 1
    feature_group[f"new_feat2"] = feat + 2
    feature_group.save()
    flist_1 = FeatureList([feat, feature_group["new_feat1"]], name="new_flist_1")
    flist_2 = FeatureList([feat, feature_group["new_feat2"]], name="new_flist_2")
    flist_1.save()
    flist_2.save()

    # check feature list class list_version & feature list object list_versions
    assert_frame_equal(
        FeatureList.list_versions(),
        pd.DataFrame(
            {
                "id": [flist_2.id, flist_1.id, saved_feature_list.id],
                "name": [flist_2.name, flist_1.name, saved_feature_list.name],
                "version": [
                    flist_2.version,
                    flist_1.version,
                    saved_feature_list.version,
                ],
                "num_feature": [2, 2, 1],
                "online_frac": [0.0] * 3,
                "deployed": [False, False, saved_feature_list.deployed],
                "created_at": [
                    flist_2.created_at,
                    flist_1.created_at,
                    saved_feature_list.created_at,
                ],
                "is_default": [True] * 3,
            }
        ),
    )

    assert_frame_equal(
        saved_feature_list.list_versions(),
        pd.DataFrame(
            {
                "id": [saved_feature_list.id],
                "name": [saved_feature_list.name],
                "version": [saved_feature_list.version],
                "online_frac": 0.0,
                "deployed": [saved_feature_list.deployed],
                "created_at": [saved_feature_list.created_at],
                "is_default": [True],
            }
        ),
    )

    # check documentation of the list_versions
    assert FeatureList.list_versions.__doc__ == FeatureList._list_versions.__doc__
    assert (
        saved_feature_list.list_versions.__doc__
        == saved_feature_list._list_versions_with_same_name.__doc__
    )


def test_get_historical_feature_sql(saved_feature_list):
    """Test get_historical_features_sql method (check it can be triggered without any error)"""
    point_in_time = pd.date_range("2001-01-01", "2001-01-02", freq="d")
    observation_set = pd.DataFrame(
        {"POINT_IN_TIME": point_in_time, "cust_id": [1234] * len(point_in_time)}
    )
    sql = saved_feature_list.get_historical_features_sql(observation_set=observation_set)
    assert 'WITH "REQUEST_TABLE_W86400_F1800_BS600_M300_cust_id" AS' in sql


def test_feature_list__feature_list_saving_with_saved_feature(
    snowflake_event_table,
    production_ready_feature,
    draft_feature,
    deprecated_feature,
):
    """Test feature list saving in bad state due to some feature has been saved (when the feature id is the same)"""
    # create a feature list
    feature_list = FeatureList(
        [
            production_ready_feature,
            draft_feature,
            deprecated_feature,
        ],
        name="feature_list_name",
    )
    assert feature_list.saved is False

    # save the feature outside the feature list
    production_ready_feature.save()
    assert production_ready_feature.saved is True

    # the feature inside the feature list saved status should be updated
    assert feature_list["production_ready_feature"].saved is True

    # try to save the feature list
    feature_list.save()
    assert feature_list.saved is True


def test_feature_list__feature_list_saving_in_bad_state__feature_id_is_different(
    snowflake_event_table,
    feature_group,
    production_ready_feature,
    draft_feature,
    deprecated_feature,
):
    """Test feature list saving in bad state due to some feature has been saved (when the feature id is different)"""
    # save the feature outside the feature list
    production_ready_feature.save()

    # create a feature list (simulate the case when the feature with the same name is created and ID are different)
    feature = feature_group["sum_30m"] + 123
    feature.name = "production_ready_feature"
    feature_list = FeatureList(
        [
            feature,
            draft_feature,
            deprecated_feature,
        ],
        name="feature_list_name",
    )

    with pytest.raises(RecordCreationException) as exc:
        feature_list.save()
    expected_msg = (
        'FeatureNamespace (name: "production_ready_feature") already exists. '
        'Please rename object (name: "production_ready_feature") to something else.'
    )
    assert expected_msg in str(exc.value)
    assert feature_list[feature.name].id == feature.id


@pytest.fixture(name="feature_list")
def feature_list_fixture(
    snowflake_event_table,
    production_ready_feature,
    draft_feature,
    deprecated_feature,
):
    # create a feature list
    feature_list = FeatureList(
        [
            production_ready_feature,
            draft_feature,
            deprecated_feature,
        ],
        name="feature_list_name",
    )
    yield feature_list


def test_feature_list_update_status_and_default_version_mode(feature_list):
    """Test update feature list status"""
    assert feature_list.saved is False
    feature_list.save()
    assert feature_list.saved is True
    assert feature_list.status == FeatureListStatus.DRAFT
    feature_list.update_status("PUBLIC_DRAFT")
    assert feature_list.status == FeatureListStatus.PUBLIC_DRAFT

    # test update on the same status
    feature_list.update_status(FeatureListStatus.PUBLIC_DRAFT)
    assert feature_list.status == FeatureListStatus.PUBLIC_DRAFT

    # check default version mode
    assert feature_list.default_version_mode == DefaultVersionMode.AUTO
    feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
    assert feature_list.default_version_mode == DefaultVersionMode.MANUAL

    # test update on wrong status input
    with pytest.raises(ValueError) as exc:
        feature_list.update_status("random")
    assert "'random' is not a valid FeatureListStatus" in str(exc.value)


def test_feature_list_update_status_and_default_version_mode__unsaved_feature_list(feature_list):
    """Test feature list status update - unsaved feature list"""
    assert feature_list.saved is False
    with pytest.raises(RecordRetrievalException) as exc:
        feature_list.update_status(FeatureListStatus.TEMPLATE)
    expected = f'FeatureList (id: "{feature_list.id}") not found. Please save the FeatureList object first.'
    assert expected in str(exc.value)

    with pytest.raises(RecordRetrievalException) as exc:
        feature_list.update_default_version_mode(DefaultVersionMode.MANUAL)
    assert expected in str(exc.value)


def _assert_all_features_in_list_with_enabled_status(feature_list, is_enabled):
    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        assert feature.online_enabled == is_enabled


def test_deploy__feature_list_with_already_production_ready_features_doesnt_error(feature_list):
    """
    Test that deploying a feature list that already has features that are production ready doesn't error.
    """
    deployments = list_deployments(include_id=True)
    expected = pd.DataFrame(
        columns=[
            "id",
            "name",
            "catalog_name",
            "feature_list_name",
            "feature_list_version",
            "num_feature",
        ]
    )
    assert_frame_equal(deployments, expected)

    # check list deployments without id
    deployments = list_deployments(include_id=False)
    assert_frame_equal(deployments, expected.drop(columns=["id"]))

    feature_list.save()
    deployment = feature_list.deploy(make_production_ready=True)
    assert deployment.enabled is False
    deployment.enable()
    _assert_all_features_in_list_with_enabled_status(feature_list, True)

    deployments = list_deployments(include_id=True)
    expected_deployment_name = f"Deployment with {feature_list.name}_{feature_list.version}"
    assert_frame_equal(
        deployments[
            ["name", "catalog_name", "feature_list_name", "feature_list_version", "num_feature"]
        ],
        pd.DataFrame(
            [
                {
                    "name": expected_deployment_name,
                    "catalog_name": "catalog",
                    "feature_list_name": feature_list.name,
                    "feature_list_version": feature_list.version,
                    "num_feature": len(feature_list.feature_names),
                }
            ]
        ),
    )

    # Deploy again to show that we don't error
    deployment.enable()
    assert deployment.enabled is True
    _assert_all_features_in_list_with_enabled_status(feature_list, True)

    # Disable feature list
    deployment.disable()
    assert deployment.enabled is False
    _assert_all_features_in_list_with_enabled_status(feature_list, False)


def test_deploy__ignore_guardrails_skips_validation_checks(feature_list, snowflake_event_table):
    """
    Test that deploying a feature list with ignore guardrails skips validation checks.
    """
    feature_list.save()
    _assert_all_features_in_list_with_enabled_status(feature_list, False)

    # Update feature job setting so that guardrails check will fail
    snowflake_event_table.update_default_feature_job_setting(
        FeatureJobSetting(blind_spot="75m", frequency="30m", time_modulo_frequency="15m")
    )

    # Deploy a feature list that errors due to guardrails
    with pytest.raises(RecordUpdateException) as exc:
        feature_list.deploy(make_production_ready=True)
    assert (
        "Discrepancies found between the promoted feature version you are trying to promote to "
        "PRODUCTION_READY, and the input table." in str(exc.value)
    )
    _assert_all_features_in_list_with_enabled_status(feature_list, False)

    # Set ignore_guardrails to be True - verify that the feature list deploys without errors
    deployment = feature_list.deploy(make_production_ready=True, ignore_guardrails=True)
    assert deployment.enabled == False
    deployment.enable()
    _assert_all_features_in_list_with_enabled_status(feature_list, True)


def test_deploy(feature_list, production_ready_feature, draft_feature, mock_api_object_cache):
    """Test feature list deployment update"""
    _ = mock_api_object_cache
    feature_list.save()
    assert feature_list.saved is True

    # create another feature list
    another_feature_list = FeatureList(
        [
            production_ready_feature,
            draft_feature,
        ],
        name="another_feature_list",
    )
    another_feature_list.save(conflict_resolution="retrieve")

    # check feature online_enabled status
    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        assert not feature.online_enabled
        assert feature.deployed_feature_list_ids == []

    # first deploy feature list
    deployment = feature_list.deploy(make_production_ready=True)
    assert deployment.enabled is False
    deployment.enable()
    assert deployment.enabled is True

    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        assert feature.online_enabled
        assert feature.deployed_feature_list_ids == [feature_list.id]

    # deploy another feature list
    another_deployment = another_feature_list.deploy()
    another_deployment.enable()
    assert another_deployment.enabled is True

    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        assert feature.online_enabled
        if feature_id in another_feature_list.feature_ids:
            # when the feature appears in both feature lists
            assert sorted(feature.deployed_feature_list_ids) == sorted(
                [feature_list.id, another_feature_list.id]
            )
        else:
            # when the feature is in one feature list only
            assert feature.deployed_feature_list_ids == [feature_list.id]

    # disable feature list deployment
    deployment.disable()
    assert deployment.enabled is False

    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        if feature_id in another_feature_list.feature_ids:
            assert feature.online_enabled
            assert feature.deployed_feature_list_ids == [another_feature_list.id]
        else:
            assert not feature.online_enabled
            assert feature.deployed_feature_list_ids == []

    # disable another feature list deployment
    another_deployment.disable()
    assert another_deployment.enabled is False

    for feature_id in feature_list.feature_ids:
        feature = Feature.get_by_id(feature_id)
        assert not feature.online_enabled
        assert feature.deployed_feature_list_ids == []


def test_get_sql(feature_list):
    """Test get sql for feature"""
    expected = textwrap.dedent(
        """
        SELECT
          (
            "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" + 123
          ) AS "production_ready_feature",
          (
            (
              "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" + 123
            ) + 123
          ) AS "draft_feature",
          (
            (
              "_fb_internal_window_w1800_sum_aed233b0e8a6e1c1e0d5427b126b03c949609481" + 123
            ) + 123
          ) AS "deprecated_feature"
        FROM _FB_AGGREGATED AS AGG
        """
    ).strip()
    assert feature_list.sql.endswith(expected)


def test_list_filter(saved_feature_list):
    """Test filters in list"""
    # test filter by table and entity
    feature_lists = FeatureList.list(table="sf_event_table")
    assert feature_lists.shape[0] == 1

    feature_lists = FeatureList.list(table="other_data", include_id=True)
    assert feature_lists.shape[0] == 0

    feature_lists = FeatureList.list(entity="customer")
    assert feature_lists.shape[0] == 1

    feature_lists = FeatureList.list(entity="other_entity")
    assert feature_lists.shape[0] == 0

    feature_lists = FeatureList.list(table="sf_event_table", entity="customer")
    assert feature_lists.shape[0] == 1

    feature_lists = FeatureList.list(table="sf_event_table", entity="other_entity")
    assert feature_lists.shape[0] == 0

    feature_lists = FeatureList.list(table="other_data", entity="customer")
    assert feature_lists.shape[0] == 0

    feature_lists = FeatureList.list(primary_entity="customer")
    assert feature_lists.shape[0] == 1

    feature_lists = FeatureList.list(primary_entity=["customer"])
    assert feature_lists.shape[0] == 1

    feature_lists = FeatureList.list(primary_entity=["customer", "other_entity"])
    assert feature_lists.shape[0] == 0


def test_save_feature_group(saved_feature_list):
    """Test feature group saving"""
    float_feature = saved_feature_list["sum_1d"]
    feature_group = FeatureGroup([])
    for idx in range(5):
        feature_group[f"feat_{idx}"] = float_feature + idx

    # check that features in feature group not saved
    for feature in feature_group.feature_objects.values():
        assert feature.saved is False

    # save feature group
    feature_group.save()

    for feature in feature_group.feature_objects.values():
        assert feature.saved is True

    # update feature group & expect record conflict error while saving the feature group
    feature_group["feat_0"] = feature_group["feat_0"] + 1
    with pytest.raises(RecordCreationException) as exc:
        feature_group.save()

    expected_msg = (
        'FeatureNamespace (name: "feat_0") already exists. '
        'Please rename object (name: "feat_0") to something else.'
    )
    assert expected_msg in str(exc.value)

    # check that "retrieve" conflict resolution works properly
    feature_group.save(conflict_resolution="retrieve")


def test_feature_list_constructor():
    """Test FeatureList constructor"""
    _ = FeatureList([], "my_fl")  # ok
    with pytest.raises(TypeError) as exc:
        FeatureList([])
    assert "missing a required argument: 'name'" in str(exc.value)


def test_list_features(saved_feature_list, float_feature):
    """
    Test list_features
    """
    feature_version_list = saved_feature_list.list_features()
    float_feature.save(conflict_resolution="retrieve")
    assert_frame_equal(
        feature_version_list,
        pd.DataFrame(
            {
                "id": [float_feature.id],
                "name": [float_feature.name],
                "version": [float_feature.version],
                "dtype": [float_feature.dtype],
                "readiness": [float_feature.readiness],
                "online_enabled": [float_feature.online_enabled],
                "tables": [["sf_event_table"]],
                "primary_tables": [["sf_event_table"]],
                "entities": [["customer"]],
                "primary_entities": [["customer"]],
                "created_at": [float_feature.created_at],
                "is_default": [True],
            }
        ),
    )

    feature_version_list = saved_feature_list.list_features()
    assert feature_version_list.shape[0] == 1


@freeze_time("2023-01-20 06:30:00")
def test_get_feature_jobs_status(saved_feature_list, feature_job_logs, update_fixtures):
    """Test get_feature_jobs_status"""
    with patch(
        "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
    ) as mock_get_jobs_dataframe:
        mock_get_jobs_dataframe.return_value = feature_job_logs
        job_status_result = saved_feature_list.get_feature_jobs_status(
            job_history_window=24, job_duration_tolerance=1700
        )

    # check request_parameters content
    assert job_status_result.request_parameters == {
        "request_date": "2023-01-20T06:30:00",
        "job_history_window": 24,
        "job_duration_tolerance": 1700,
    }

    # check feature_job_summary content
    expected_feature_job_summary = pd.DataFrame(
        {
            "aggregation_hash": {0: "aed233b0"},
            "frequency(min)": {0: 30},
            "completed_jobs": {0: 23},
            "max_duration(s)": {0: 1582.072},
            "95 percentile": {0: 1574.2431},
            "frac_late": {0: 0.0},
            "exceed_period": {0: 0},
            "failed_jobs": {0: 1},
            "incomplete_jobs": {0: 23},
            "time_since_last": {0: "29 minutes"},
        }
    )
    assert_frame_equal(job_status_result.feature_job_summary, expected_feature_job_summary)

    # check repr
    assert repr(job_status_result) == "\n\n".join(
        [
            str(pd.DataFrame.from_dict([job_status_result.request_parameters])),
            str(job_status_result.feature_tile_table),
            str(job_status_result.feature_job_summary),
        ]
    )

    # check repr html without matplotlib
    fixture_path = "tests/fixtures/feature_job_status/expected_repr.html"
    repr_html = job_status_result._repr_html_()
    if update_fixtures:
        with open(fixture_path, "w") as file_handle:
            file_handle.write(repr_html)
    else:
        with open(fixture_path, "r") as file_handle:
            expected_repr_html = file_handle.read()
            assert repr_html == expected_repr_html.strip()

    # check session logs
    fixture_path = "tests/fixtures/feature_job_status/expected_session_logs.parquet"
    if update_fixtures:
        job_status_result.job_session_logs.to_parquet(fixture_path)
    else:
        expected_session_logs = pd.read_parquet(fixture_path)
        assert_frame_equal(job_status_result.job_session_logs, expected_session_logs)

    if update_fixtures:
        raise ValueError("Fixtures updated. Please run test again without --update-fixtures flag")


def test_get_feature_jobs_status_feature_without_tile(
    saved_scd_table,
    cust_id_entity,
    snowflake_event_table,
    float_feature,
    feature_job_logs,
):
    """
    Test get_feature_jobs_status for feature without tile
    """
    saved_scd_table["col_text"].as_entity(cust_id_entity.name)

    scd_view = saved_scd_table.get_view()
    feature = scd_view["effective_timestamp"].as_feature("Latest Record Change Date")
    feature_list = FeatureList([feature, float_feature], name="FeatureList")
    feature_list.save()

    with patch(
        "featurebyte.session.snowflake.SnowflakeSession.execute_query"
    ) as mock_execute_query:
        mock_execute_query.return_value = feature_job_logs[:0]
        job_status_result = feature_list.get_feature_jobs_status()

    assert job_status_result.feature_tile_table.shape == (1, 2)
    assert job_status_result.feature_job_summary.shape == (1, 10)
    assert job_status_result.job_session_logs.shape == (0, 12)


def test_feature_list__check_feature_readiness_update(saved_feature_list, mock_api_object_cache):
    """Test feature list feature readiness derived attributes update logic"""
    _ = mock_api_object_cache
    new_feat = saved_feature_list["sum_1d"] + 100
    new_feat.name = "new_feat"
    new_feat.save()

    feature_list = FeatureList([new_feat], name="my_fl")
    assert feature_list.production_ready_fraction == 0.0
    assert feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "DRAFT", "count": 1}]
    }

    new_feat.update_readiness(readiness="PRODUCTION_READY")
    assert feature_list.production_ready_fraction == 1.0
    assert feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "PRODUCTION_READY", "count": 1}]
    }

    feature_list.save()
    assert feature_list.production_ready_fraction == 1.0
    assert feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "PRODUCTION_READY", "count": 1}]
    }

    new_feat.update_readiness(readiness="PUBLIC_DRAFT")
    assert feature_list.production_ready_fraction == 0.0
    assert feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "PUBLIC_DRAFT", "count": 1}]
    }


def test_feature_list_synchronization(saved_feature_list, mock_api_object_cache):
    """Test feature list synchronization"""
    _ = mock_api_object_cache
    # construct a cloned feature list (feature list with the same feature list ID)
    cloned_feat_list = FeatureList.get_by_id(id=saved_feature_list.id)

    # update the original feature list's version mode (stored at feature list namespace record)
    target_mode = DefaultVersionMode.MANUAL
    assert saved_feature_list.default_version_mode != target_mode
    saved_feature_list.update_default_version_mode(target_mode)
    assert saved_feature_list.default_version_mode == target_mode

    # check the clone's version mode also get updated
    assert cloned_feat_list.default_version_mode == target_mode

    # update original feature list deployed status (stored at feature list record)
    assert saved_feature_list.deployed is False
    assert saved_feature_list["sum_1d"].readiness == FeatureReadiness.DRAFT
    deployment = saved_feature_list.deploy(make_production_ready=True)
    deployment.enable()
    assert deployment.enabled == True
    assert saved_feature_list["sum_1d"].readiness == FeatureReadiness.PRODUCTION_READY
    assert saved_feature_list.deployed is True

    # check the clone's deployed value
    assert cloned_feat_list.deployed is True
    assert cloned_feat_list["sum_1d"].readiness == FeatureReadiness.PRODUCTION_READY


def test_feature_list_properties_from_cached_model__before_save(feature_list):
    """Test (unsaved) feature list properties from cached model"""
    # check properties derived from feature list model directly
    assert feature_list.saved is False
    assert feature_list.online_enabled_feature_ids == []
    assert feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "DRAFT", "count": 3}]
    }
    assert feature_list.production_ready_fraction == 0.0
    assert feature_list.deployed is False

    # check properties use feature list namespace model info
    props = ["is_default", "default_version_mode", "status"]
    for prop in props:
        with pytest.raises(RecordRetrievalException):
            _ = getattr(feature_list, prop)


def test_feature_list_properties_from_cached_model__after_save(saved_feature_list):
    """Test (saved) feature list properties from cached model"""
    # check properties derived from feature list model directly
    assert saved_feature_list.saved
    assert saved_feature_list.online_enabled_feature_ids == []
    assert saved_feature_list.readiness_distribution.dict() == {
        "__root__": [{"readiness": "DRAFT", "count": 1}]
    }
    assert saved_feature_list.production_ready_fraction == 0.0
    assert saved_feature_list.deployed is False

    # check properties use feature list namespace model info
    assert saved_feature_list.is_default is True
    assert saved_feature_list.default_version_mode == DefaultVersionMode.AUTO
    assert saved_feature_list.status == FeatureListStatus.DRAFT


def test_delete_feature_list_namespace__success(saved_feature_list):
    """Test delete feature list namespace (success)"""
    # check feature list ids before delete
    feature = saved_feature_list["sum_1d"]
    assert feature.feature_list_ids == [saved_feature_list.id]

    assert saved_feature_list.status == FeatureListStatus.DRAFT
    saved_feature_list.delete()
    assert saved_feature_list.saved is False

    # check feature list ids after delete
    assert feature.feature_list_ids == []

    # check feature list namespace & feature list records are deleted
    with pytest.raises(RecordRetrievalException) as exc_info:
        FeatureListNamespace.get_by_id(saved_feature_list.feature_list_namespace.id)

    expected_msg = (
        f'FeatureList (id: "{saved_feature_list.id}") not found. '
        "Please save the FeatureList object first."
    )
    assert expected_msg in str(exc_info.value)

    with pytest.raises(RecordRetrievalException) as exc_info:
        FeatureList.get_by_id(saved_feature_list.id)

    expected_msg = (
        f'FeatureList (id: "{saved_feature_list.id}") not found. '
        "Please save the FeatureList object first."
    )
    assert expected_msg in str(exc_info.value)


def test_delete_feature_list(saved_feature_list):
    """Test delete feature list"""
    assert saved_feature_list.status == FeatureListStatus.DRAFT
    saved_feature_list.delete()
    assert saved_feature_list.saved is False

    with pytest.raises(RecordRetrievalException) as exc_info:
        FeatureList.get_by_id(saved_feature_list.id)

    expected_msg = f'FeatureList (id: "{saved_feature_list.id}") not found. Please save the FeatureList object first.'
    assert expected_msg in str(exc_info.value)


def test_delete_feature_list__failure(saved_feature_list):
    """Test delete feature list (failure)"""
    saved_feature_list.update_status(FeatureListStatus.PUBLIC_DRAFT)
    assert saved_feature_list.status == FeatureListStatus.PUBLIC_DRAFT

    with pytest.raises(RecordDeletionException) as exc_info:
        saved_feature_list.delete()

    expected_msg = "Only feature list with DRAFT status can be deleted."
    assert expected_msg in str(exc_info.value)


def test_primary_entity__unsaved_feature_list(feature_list, cust_id_entity):
    """
    Test primary_entity attribute for an unsaved feature list
    """
    assert feature_list.primary_entity == [Entity.get_by_id(cust_id_entity.id)]


def test_primary_entity__saved_feature_list(saved_feature_list, cust_id_entity):
    """
    Test primary_entity attribute for an unsaved feature list
    """
    assert saved_feature_list.primary_entity == [Entity.get_by_id(cust_id_entity.id)]


def test_feature_list__features_order_is_kept(float_feature, non_time_based_feature):
    """Test feature list features order is kept after save"""
    # save features first
    float_feature.save()
    non_time_based_feature.save()

    feature_list_1 = FeatureList([float_feature, non_time_based_feature], "test_feature_list_1")
    feature_list_1.save()
    assert feature_list_1.feature_names == [float_feature.name, non_time_based_feature.name]

    feature_list_2 = FeatureList([non_time_based_feature, float_feature], "test_feature_list_2")
    feature_list_2.save()
    assert feature_list_2.feature_names == [non_time_based_feature.name, float_feature.name]


def test_feature_list_save__different_feature_are_used_due_to_conflict_resolution(float_feature):
    """
    Test feature list saving in bad state due to some feature has been saved (when the feature id is different)
    """
    feat_name = float_feature.name
    new_feat = float_feature + 123
    new_feat.name = feat_name
    new_feat.save()
    assert new_feat.saved

    # check that metadata of the feature info is not empty
    assert new_feat.info()["metadata"] is not None

    # check the different feature is used to save the feature list if conflict_resolution is set to "retrieve"
    feature_list = FeatureList([float_feature], name="my_fl")
    feature_list.save(conflict_resolution="retrieve")
    assert feature_list[feat_name].id == new_feat.id


def test_feature_list_save__check_feature_list_ids_correctly_set(float_feature):
    """Test feature_list_ids are correctly set"""
    float_feature.save()
    assert float_feature.feature_list_ids == []

    # save a new feature list
    feature_list = FeatureList([float_feature], name="test_feature_list")
    feature_list.save()

    # check feature_list_ids are correctly set
    assert float_feature.feature_list_ids == [feature_list.id]

    # delete the feature list
    feature_list.delete()

    # check feature_list_ids are correctly set
    assert float_feature.feature_list_ids == []


def test_update_description(saved_feature_list):
    """Test update description"""
    assert saved_feature_list.description is None
    saved_feature_list.update_description("new description")
    assert saved_feature_list.description == "new description"
    assert saved_feature_list.info()["description"] == "new description"
    saved_feature_list.update_description(None)
    assert saved_feature_list.description is None
    assert saved_feature_list.info()["description"] is None

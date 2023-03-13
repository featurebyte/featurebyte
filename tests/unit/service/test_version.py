"""
Test for VersionService
"""
import json
import os

import pytest
import pytest_asyncio
from bson import ObjectId
from pydantic import ValidationError

from featurebyte.common.model_util import get_version
from featurebyte.exception import (
    DocumentError,
    NoChangesInFeatureVersionError,
    NoFeatureJobSettingInSourceError,
)
from featurebyte.models.feature_list import FeatureListNewVersionMode
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.feature_job_setting import (
    DataFeatureJobSetting,
    FeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    DataCleaningOperation,
    MissingValueImputation,
)
from featurebyte.schema.event_data import EventDataServiceUpdate
from featurebyte.schema.feature import FeatureCreate, FeatureNewVersionCreate
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListNewVersionCreate,
    FeatureVersionInfo,
)


def test_feature_new_version_create_schema_validation():
    """Test feature new version create schema validation"""
    with pytest.raises(ValidationError) as exc:
        FeatureNewVersionCreate(
            source_feature_id=ObjectId(),
            data_cleaning_operations=[
                DataCleaningOperation(data_name="dup_data_name", column_cleaning_operations=[]),
                DataCleaningOperation(data_name="dup_data_name", column_cleaning_operations=[]),
            ],
        )

    expected_error = 'Name "dup_data_name" is duplicated (field: data_name).'
    assert expected_error in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        FeatureNewVersionCreate(
            source_feature_id=ObjectId(),
            data_cleaning_operations=[
                DataCleaningOperation(
                    data_name="data_name",
                    column_cleaning_operations=[
                        ColumnCleaningOperation(column_name="dup_col_name", cleaning_operations=[]),
                        ColumnCleaningOperation(column_name="dup_col_name", cleaning_operations=[]),
                    ],
                ),
            ],
        )

    expected_error = 'Name "dup_col_name" is duplicated (field: column_name).'
    assert expected_error in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_version(
    version_service, feature, feature_namespace_service, api_object_to_id
):
    """Test create new feature version"""
    version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            data_feature_job_settings=[
                DataFeatureJobSetting(
                    data_name="sf_event_data",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
                    ),
                )
            ],
        )
    )

    # compare groupby node
    expected_common_params = {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "value_by": None,
        "windows": ["30m"],
        "timestamp": "event_timestamp",
        "names": ["sum_30m"],
        "serving_names": ["cust_id"],
        "entity_ids": [ObjectId(api_object_to_id["entity"])],
    }
    parameters = feature.graph.get_node_by_name("groupby_1").parameters
    assert parameters.dict() == {
        **expected_common_params,
        "blind_spot": 600,
        "time_modulo_frequency": 300,
        "frequency": 1800,
        "tile_id": "TILE_F1800_M300_B600_99CB16A0CBF5645D5C2D1DEA5CA74D4BD1660817",
        "aggregation_id": "sum_60e19c3e160be7db3a64f2a828c1c7929543abb4",
    }

    new_parameters = version.graph.get_node_by_name("groupby_1").parameters
    assert new_parameters.dict() == {
        **expected_common_params,
        "blind_spot": 86400,
        "time_modulo_frequency": 3600,
        "frequency": 86400,
        "tile_id": "TILE_F86400_M3600_B86400_AEA0B3C1AB23571D22B2280D44878244BE758181",
        "aggregation_id": "sum_c7b86857737ce503c31f94a87b112f2193c9a02a",
    }

    # compare edges & other nodes
    assert feature.graph.edges == version.graph.edges
    for node_name in ["input_1", "project_1"]:
        assert feature.graph.get_node_by_name(node_name) == version.graph.get_node_by_name(
            node_name
        )
    _ = feature

    # check version
    version_name = get_version()
    assert feature.version == {"name": version_name, "suffix": None}
    assert version.version == {"name": version_name, "suffix": 1}

    # check other attributes
    assert version.node_name == "project_1"
    assert version.readiness == "DRAFT"
    assert version.feature_namespace_id == feature.feature_namespace_id
    assert version.entity_ids == feature.entity_ids
    assert version.feature_list_ids == []
    assert version.deployed_feature_list_ids == []
    assert version.online_enabled is False

    # check feature namespace service get updated
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.feature_ids == [feature.id, version.id]


@pytest.mark.asyncio
async def test_create_new_feature_version__document_error(version_service, feature, event_data):
    """Test create new feature version (document error due to no change is detected)"""
    # check no feature job settings
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(source_feature_id=feature.id, feature_job_setting=None)
        )

    expected_msg = "No change detected on the new feature version."
    assert expected_msg in str(exc.value)

    # check with empty data cleaning operations
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(source_feature_id=feature.id, data_cleaning_operations=[])
        )

    assert expected_msg in str(exc.value)

    # check same feature job settings
    same_feature_job_setting = FeatureJobSetting(
        blind_spot="10m", frequency="30m", time_modulo_frequency="5m"
    )
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                data_feature_job_settings=[
                    DataFeatureJobSetting(
                        data_name=event_data.name, feature_job_setting=same_feature_job_setting
                    )
                ],
            ),
        )

    expected_msg = (
        "Feature job setting does not result a new feature version. "
        "This is because the new feature version is the same as the source feature."
    )
    assert expected_msg in str(exc.value)

    # check data cleaning operations with no effect in feature value derivation
    no_effect_data_cleaning_operations = [
        DataCleaningOperation(
            data_name=event_data.name,
            column_cleaning_operations=[
                ColumnCleaningOperation(
                    column_name="col_int",  # column is not used in this feature
                    cleaning_operations=[MissingValueImputation(imputed_value=0.0)],
                )
            ],
        )
    ]
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                data_cleaning_operations=no_effect_data_cleaning_operations,
            )
        )

    expected_msg = "Data cleaning operation(s) does not result a new feature version."
    assert expected_msg in str(exc.value)

    # check feature job setting and data cleaning operations with no effect in feature value derivation
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                data_feature_job_settings=[
                    DataFeatureJobSetting(
                        data_name=event_data.name,
                        feature_job_setting=same_feature_job_setting,
                    )
                ],
                data_cleaning_operations=no_effect_data_cleaning_operations,
            )
        )

    expected_msg = (
        "Feature job setting and data cleaning operation(s) do not result a new feature version."
    )
    assert expected_msg in str(exc.value)


@pytest_asyncio.fixture(name="feature_sum_2h")
async def feature_sum_2h_fixture(test_dir, feature_service, feature):
    """Feature sum_2h fixture"""
    _ = feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_2h.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_list_multi")
async def feature_list_fixture(test_dir, feature, feature_sum_2h, feature_list_service):
    """Feature list model"""
    _ = feature, feature_sum_2h
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_multi.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = await feature_list_service.create_document(data=FeatureListCreate(**payload))
        return feature_list


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__no_change_detected(
    version_service, feature_list_multi
):
    """Test create new feature version (error due to no change detected)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.AUTO,
            ),
        )

    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__feature_info_is_missing(
    version_service, feature_list_multi
):
    """Test create new feature version (error due to feature info is missing)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.MANUAL,
            ),
        )
    expected_msg = "Feature info is missing."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__unexpected_feature_info(
    version_service, feature_list, feature_sum_2h
):
    """Test create new feature version (error due to unexpected feature)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list.id,
                mode=FeatureListNewVersionMode.MANUAL,
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
        )
    expected_msg = 'Features ("sum_2h") are not in the original FeatureList'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__auto_mode(
    version_service,
    event_data,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
):
    """Test create new feature version (auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            data_feature_job_settings=[
                DataFeatureJobSetting(
                    data_name=event_data.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
                    ),
                )
            ],
        ),
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.AUTO,
        ),
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature_sum_2h.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=new_flist_version.id,
                mode=FeatureListNewVersionMode.AUTO,
            ),
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__manual_mode(
    version_service,
    event_data,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
):
    """Test create new feature version (manual mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_sum_2h.id,
            data_feature_job_settings=[
                DataFeatureJobSetting(
                    data_name=event_data.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
                    ),
                )
            ],
        ),
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.MANUAL,
            features=[
                FeatureVersionInfo(name=new_feat_version.name, version=new_feat_version.version)
            ],
        ),
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=new_flist_version.id,
                mode=FeatureListNewVersionMode.MANUAL,
                features=[
                    FeatureVersionInfo(name=new_feat_version.name, version=new_feat_version.version)
                ],
            ),
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__semi_auto_mode(
    version_service,
    event_data,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
):
    """Test create new feature version (semi-auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_sum_2h.id,
            data_feature_job_settings=[
                DataFeatureJobSetting(
                    data_name=event_data.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
                    ),
                )
            ],
        ),
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id, return_document=True
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            mode=FeatureListNewVersionMode.SEMI_AUTO,
            features=[FeatureVersionInfo(name=feature.name, version=feature.version)],
        ),
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                mode=FeatureListNewVersionMode.SEMI_AUTO,
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
        )
    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


def create_data_cleaning_operations(data_name, column_names):
    """Create data cleaning operations ofr a given data and column"""
    return DataCleaningOperation(
        data_name=data_name,
        column_cleaning_operations=[
            ColumnCleaningOperation(
                column_name=column_name,
                cleaning_operations=[
                    MissingValueImputation(imputed_value=0.0),
                ],
            )
            for column_name in column_names
        ],
    )


@pytest.mark.asyncio
async def test_create_new_feature_version__with_event_data_cleaning_operations(
    version_service, feature, event_data
):
    """Test create new feature version with event data cleaning operations"""
    _ = event_data
    version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            data_cleaning_operations=[
                create_data_cleaning_operations(event_data.name, ["col_float"])
            ],
        )
    )

    # check newly created version
    assert (
        version.graph.edges
        == feature.graph.edges
        == [
            {"source": "input_1", "target": "graph_1"},
            {"source": "graph_1", "target": "groupby_1"},
            {"source": "groupby_1", "target": "project_1"},
        ]
    )

    # check view graph node
    updated_view_graph_node = version.graph.get_node_by_name("graph_1")
    original_view_graph_node = feature.graph.get_node_by_name("graph_1")
    assert original_view_graph_node.parameters.graph.edges_map == {"proxy_input_1": ["project_1"]}
    assert updated_view_graph_node.parameters.graph.edges_map == {
        "proxy_input_1": ["project_1"],
        "project_1": ["graph_1"],
    }

    # check cleaning graph node
    cleaning_graph_node = updated_view_graph_node.parameters.graph.get_node_by_name("graph_1")
    assert cleaning_graph_node.parameters.graph.edges_map == {
        "proxy_input_1": ["project_1", "assign_1"],
        # is null node is used in missing value imputation
        "project_1": ["is_null_1", "conditional_1"],
        "is_null_1": ["conditional_1"],
        "conditional_1": ["cast_1"],
        "cast_1": ["assign_1"],
    }

    # check that the assign column name is expected
    assign_node = cleaning_graph_node.parameters.graph.get_node_by_name("assign_1")
    assert assign_node.parameters.name == "col_float"


@pytest.mark.asyncio
async def test_create_new_feature_version__document_error_with_item_data_cleaning_operations(
    version_service, feature_non_time_based, event_data, item_data
):
    """Test create new feature version with event data cleaning operations (document error)"""
    # create a new feature version with irrelevant data cleaning operations
    # feature_non_time_based has the following definition:
    # feat = item_view.groupby(by_keys=["event_id_col"], category=None).aggregate(
    #     value_column="item_amount",
    #     method="sum",
    #     feature_name="non_time_time_sum_amount_feature",
    #     skip_fill_na=True,
    event_data_columns = ["col_float", "col_char", "col_text"]
    item_data_columns = ["item_id_col", "item_type"]
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature_non_time_based.id,
                data_cleaning_operations=[
                    create_data_cleaning_operations(event_data.name, event_data_columns),
                    create_data_cleaning_operations(item_data.name, item_data_columns),
                ],
            )
        )
    expected_msg = "Data cleaning operation(s) does not result a new feature version."
    assert expected_msg in str(exc.value)


@pytest.fixture(name="event_metadata")
def event_metadata_fixture(event_data):
    """Event metadata"""
    return {
        "view_mode": "auto",
        "drop_column_names": ["created_at"],
        "column_cleaning_operations": [],
        "data_id": event_data.id,
    }


@pytest.fixture(name="item_metadata")
def item_metadata_fixture(item_data, event_metadata, event_data):
    """Item metadata"""
    return {
        "view_mode": "auto",
        "drop_column_names": [],
        "column_cleaning_operations": [],
        "data_id": item_data.id,
        "event_suffix": "_event_table",
        "event_drop_column_names": event_metadata["drop_column_names"],
        "event_column_cleaning_operations": event_metadata["column_cleaning_operations"],
        "event_data_id": event_data.id,
        "event_join_column_names": ["event_timestamp", "col_int", "cust_id"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data_cleaning_operations, expected_event_metadata, expected_item_metadata",
    [
        # case 1: update item data only
        (
            [create_data_cleaning_operations("sf_item_data", ["item_amount"])],
            {},  # no change in event metadata
            {
                "column_cleaning_operations": [
                    {
                        "column_name": "item_amount",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
        ),
        # case 2: update event data only
        (
            [create_data_cleaning_operations("sf_event_data", ["col_int"])],
            {
                "column_cleaning_operations": [
                    {
                        "column_name": "col_int",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
            {
                "event_column_cleaning_operations": [
                    {
                        "column_name": "col_int",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
        ),
        # case 3: update event data & item data cleaning operations
        (
            [
                create_data_cleaning_operations("sf_event_data", ["col_int"]),
                create_data_cleaning_operations("sf_item_data", ["item_amount"]),
            ],
            {
                "column_cleaning_operations": [
                    {
                        "column_name": "col_int",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
            {
                "column_cleaning_operations": [
                    {
                        "column_name": "item_amount",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
                "event_column_cleaning_operations": [
                    {
                        "column_name": "col_int",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
        ),
    ],
)
async def test_create_new_feature_version__with_non_time_based_feature(
    version_service,
    feature_non_time_based,
    event_metadata,
    item_metadata,
    data_cleaning_operations,
    expected_event_metadata,
    expected_item_metadata,
):
    """Test create new feature version with event data cleaning operations"""
    event_view_graph_node = feature_non_time_based.graph.get_node_by_name("graph_1")
    item_view_graph_node = feature_non_time_based.graph.get_node_by_name("graph_2")
    assert event_view_graph_node.parameters.metadata == event_metadata
    assert item_view_graph_node.parameters.metadata == item_metadata

    # create a new feature version with relevant data cleaning operations
    new_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_non_time_based.id,
            data_cleaning_operations=data_cleaning_operations,
        )
    )

    # check graph node metadata
    new_event_view_graph_node = new_version.graph.get_node_by_name("graph_1")
    new_item_view_graph_node = new_version.graph.get_node_by_name("graph_2")
    expected_event_metadata = {**event_metadata, **expected_event_metadata}
    expected_item_metadata = {**item_metadata, **expected_item_metadata}
    assert new_event_view_graph_node.parameters.metadata.dict() == expected_event_metadata
    assert new_item_view_graph_node.parameters.metadata.dict() == expected_item_metadata

    # check consistencies
    event_metadata = new_event_view_graph_node.parameters.metadata
    item_metadata = new_item_view_graph_node.parameters.metadata
    assert (
        item_metadata.event_column_cleaning_operations == event_metadata.column_cleaning_operations
    )
    assert item_metadata.event_drop_column_names == event_metadata.drop_column_names

    # graph structure (edges) should be the same
    assert new_version.graph.edges == feature_non_time_based.graph.edges


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings(
    version_service, event_data_service, feature, event_data
):
    """Test create new feature version using source settings"""
    # check current feature settings
    view_graph_params = feature.graph.get_node_by_name("graph_1").parameters
    assert view_graph_params.metadata.column_cleaning_operations == []

    group_by_params = feature.graph.get_node_by_name("groupby_1").parameters
    assert group_by_params.blind_spot == 600
    assert group_by_params.frequency == 1800
    assert group_by_params.time_modulo_frequency == 300

    # prepare event data before create new version from source settings
    columns_info_with_cdi = []
    for col in event_data.columns_info:
        if col.name == "col_float":
            col.critical_data_info = CriticalDataInfo(
                cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
            )
        columns_info_with_cdi.append(col)

    await event_data_service.update_document(
        document_id=event_data.id,
        data=EventDataServiceUpdate(
            default_feature_job_setting=FeatureJobSetting(
                blind_spot="1h", frequency="2h", time_modulo_frequency="30m"
            ),
            columns_info=columns_info_with_cdi,
        ),
    )

    # create new version from source settings & check the feature job setting & data cleaning operations
    new_version = await version_service.create_new_feature_version_using_source_settings(
        document_id=feature.id
    )
    view_graph_params = new_version.graph.get_node_by_name("graph_1").parameters
    assert view_graph_params.metadata.column_cleaning_operations == [
        ColumnCleaningOperation(
            column_name="col_float", cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
        )
    ]

    group_by_params = new_version.graph.get_node_by_name("groupby_1").parameters
    assert group_by_params.blind_spot == 3600
    assert group_by_params.frequency == 7200
    assert group_by_params.time_modulo_frequency == 1800


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings__no_changes_throws_error(
    version_service,
    feature,
):
    """
    Test that creating a new feature version using source that doesn't have a feature job setting will throw an error.
    """
    with pytest.raises(NoChangesInFeatureVersionError) as exc:
        await version_service.create_new_feature_version_using_source_settings(feature.id)
    assert "No change detected on the new feature version" in str(exc)


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings__no_changes_in_fjs_throws_error(
    version_service, event_data_factory, feature_factory
):
    """
    Test that creating a new feature version using source that doesn't have a feature job setting will throw an error.
    """
    await event_data_factory(True)
    feature = await feature_factory()

    with pytest.raises(NoFeatureJobSettingInSourceError) as exc:
        await version_service.create_new_feature_version_using_source_settings(feature.id)
    assert "No feature job setting found in source" in str(exc)

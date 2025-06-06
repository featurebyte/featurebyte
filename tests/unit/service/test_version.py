"""
Test for VersionService
"""

import json
import os

import pytest
import pytest_asyncio
from bson import ObjectId
from pydantic import ValidationError

from featurebyte import CronFeatureJobSetting
from featurebyte.common.model_util import get_version
from featurebyte.exception import (
    DocumentError,
    DocumentNotFoundError,
    NoChangesInFeatureVersionError,
    NoFeatureJobSettingInSourceError,
)
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.feature_job_setting import (
    CalendarWindow,
    FeatureJobSetting,
    TableFeatureJobSetting,
    TableIdFeatureJobSetting,
)
from featurebyte.query_graph.node.cleaning_operation import (
    ColumnCleaningOperation,
    MissingValueImputation,
    TableCleaningOperation,
)
from featurebyte.schema.event_table import EventTableServiceUpdate
from featurebyte.schema.feature import FeatureNewVersionCreate, FeatureServiceCreate
from featurebyte.schema.feature_list import (
    FeatureListNewVersionCreate,
    FeatureListServiceCreate,
    FeatureVersionInfo,
)
from tests.util.helper import compare_pydantic_obj


def test_feature_new_version_create_schema_validation():
    """Test feature new version create schema validation"""
    with pytest.raises(ValidationError) as exc:
        FeatureNewVersionCreate(
            source_feature_id=ObjectId(),
            table_cleaning_operations=[
                TableCleaningOperation(table_name="dup_data_name", column_cleaning_operations=[]),
                TableCleaningOperation(table_name="dup_data_name", column_cleaning_operations=[]),
            ],
        )

    expected_error = 'Name "dup_data_name" is duplicated (field: table_name).'
    assert expected_error in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        FeatureNewVersionCreate(
            source_feature_id=ObjectId(),
            table_cleaning_operations=[
                TableCleaningOperation(
                    table_name="table_name",
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
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
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
        "offset": None,
        "timestamp": "event_timestamp",
        "timestamp_metadata": None,
        "names": ["sum_30m"],
        "serving_names": ["cust_id"],
        "entity_ids": [ObjectId(api_object_to_id["entity"])],
    }
    parameters = feature.graph.get_node_by_name("groupby_1").parameters
    assert parameters.model_dump() == {
        **expected_common_params,
        "feature_job_setting": {
            "blind_spot": "600s",
            "offset": "300s",
            "period": "1800s",
            "execution_buffer": "0s",
        },
        "tile_id": "TILE_SUM_E8C51D7D1EC78E1F35195FC0CF61221B3F830295",
        "tile_id_version": 2,
        "aggregation_id": "sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295",
    }

    new_parameters = version.graph.get_node_by_name("groupby_1").parameters
    assert new_parameters.model_dump() == {
        **expected_common_params,
        "feature_job_setting": {
            "blind_spot": "86400s",
            "offset": "3600s",
            "period": "86400s",
            "execution_buffer": "0s",
        },
        "tile_id": "TILE_SUM_4955D583DA1636F4125D56D20D80CD6DD0A73DEC",
        "tile_id_version": 2,
        "aggregation_id": "sum_4955d583da1636f4125d56d20d80cd6dd0a73dec",
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
    compare_pydantic_obj(feature.version, {"name": version_name, "suffix": None})
    compare_pydantic_obj(version.version, {"name": version_name, "suffix": 1})

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
async def test_create_new_feature_version__document_error(version_service, feature, event_table):
    """Test create new feature version (document error due to no change is detected)"""
    # check no feature job settings
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(source_feature_id=feature.id, feature_job_setting=None)
        )

    expected_msg = "No change detected on the new feature version."
    assert expected_msg in str(exc.value)

    # check with empty table cleaning operations
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(source_feature_id=feature.id, table_cleaning_operations=[])
        )

    assert expected_msg in str(exc.value)

    # check same feature job settings
    same_feature_job_setting = FeatureJobSetting(blind_spot="10m", period="30m", offset="5m")
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                table_feature_job_settings=[
                    TableFeatureJobSetting(
                        table_name=event_table.name, feature_job_setting=same_feature_job_setting
                    )
                ],
            ),
        )

    expected_msg = (
        "Feature job setting does not result a new feature version. "
        "This is because the new feature version is the same as the source feature."
    )
    assert expected_msg in str(exc.value)

    # check table cleaning operations with no effect in feature value derivation
    no_effect_data_cleaning_operations = [
        TableCleaningOperation(
            table_name=event_table.name,
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
                table_cleaning_operations=no_effect_data_cleaning_operations,
            )
        )

    expected_msg = "Table cleaning operation(s) does not result a new feature version."
    assert expected_msg in str(exc.value)

    # check feature job setting and table cleaning operations with no effect in feature value derivation
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature.id,
                table_feature_job_settings=[
                    TableFeatureJobSetting(
                        table_name=event_table.name,
                        feature_job_setting=same_feature_job_setting,
                    )
                ],
                table_cleaning_operations=no_effect_data_cleaning_operations,
            )
        )

    expected_msg = (
        "Feature job setting and table cleaning operation(s) do not result a new feature version."
    )
    assert expected_msg in str(exc.value)


@pytest_asyncio.fixture(name="feature_sum_2h")
async def feature_sum_2h_fixture(test_dir, feature_service, feature):
    """Feature sum_2h fixture"""
    _ = feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_2h.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureServiceCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="feature_list_multi")
async def feature_list_fixture(test_dir, feature, feature_sum_2h, feature_list_service):
    """Feature list model"""
    _ = feature, feature_sum_2h
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_multi.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = None
        try:
            feature_list = await feature_list_service.create_document(
                data=FeatureListServiceCreate(**payload)
            )
            yield feature_list
        finally:
            if feature_list:
                await feature_list_service.delete_document(document_id=feature_list.id)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__document_error__no_change_detected(
    version_service, feature_list_multi
):
    """Test create new feature version (error due to no change detected)"""
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id, features=[]
            ),
        )

    expected_msg = "No change detected on the new feature list version."
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
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
        )
    expected_msg = 'Features ("sum_2h") are not in the original FeatureList'
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__without_specifying_features_mode(
    version_service,
    event_table,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
):
    """Test create new feature version (auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=event_table.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
                    ),
                )
            ],
        ),
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(source_feature_list_id=feature_list_multi.id, features=[]),
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature_sum_2h.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=new_flist_version.id, features=[]
            ),
        )

    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_create_new_feature_list_version__specifying_features(
    version_service,
    event_table,
    feature,
    feature_sum_2h,
    feature_list_multi,
    feature_readiness_service,
):
    """Test create new feature version (semi-auto mode)"""
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_sum_2h.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=event_table.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
                    ),
                )
            ],
        ),
    )
    feat_namespace = await feature_readiness_service.update_feature_namespace(
        feature_namespace_id=new_feat_version.feature_namespace_id
    )
    assert feat_namespace.default_feature_id == new_feat_version.id

    new_flist_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list_multi.id,
            features=[FeatureVersionInfo(name=feature.name, version=feature.version)],
        ),
    )
    assert sorted(new_flist_version.feature_ids) == sorted([new_feat_version.id, feature.id])

    # check document error (due to no change is detected)
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_list_version(
            data=FeatureListNewVersionCreate(
                source_feature_list_id=feature_list_multi.id,
                features=[
                    FeatureVersionInfo(name=feature_sum_2h.name, version=feature_sum_2h.version)
                ],
            ),
        )

    expected_msg = "No change detected on the new feature list version."
    assert expected_msg in str(exc.value)


def create_table_cleaning_operations(data_name, column_names):
    """Create table cleaning operations ofr a given table and column"""
    return TableCleaningOperation(
        table_name=data_name,
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
async def test_create_new_feature_version__with_event_table_cleaning_operations(
    version_service, feature, event_table
):
    """Test create new feature version with event table cleaning operations"""
    _ = event_table
    version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_cleaning_operations=[
                create_table_cleaning_operations(event_table.name, ["col_float"])
            ],
        )
    )

    # check newly created version
    compare_pydantic_obj(version.graph.edges, expected=feature.graph.edges)
    compare_pydantic_obj(
        feature.graph.edges,
        expected=[
            {"source": "input_1", "target": "graph_1"},
            {"source": "graph_1", "target": "groupby_1"},
            {"source": "groupby_1", "target": "project_1"},
        ],
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
async def test_create_new_feature_version__document_error_with_item_table_cleaning_operations(
    version_service, feature_non_time_based, event_table, item_table
):
    """Test create new feature version with event table cleaning operations (document error)"""
    # create a new feature version with irrelevant table cleaning operations
    # feature_non_time_based has the following definition:
    # feat = item_view.groupby(by_keys=["event_id_col"], category=None).aggregate(
    #     value_column="item_amount",
    #     method="sum",
    #     feature_name="non_time_time_sum_amount_feature",
    #     skip_fill_na=True,
    event_table_columns = ["col_float", "col_char", "col_text"]
    item_table_columns = ["item_id_col", "item_type"]
    with pytest.raises(DocumentError) as exc:
        await version_service.create_new_feature_version(
            data=FeatureNewVersionCreate(
                source_feature_id=feature_non_time_based.id,
                table_cleaning_operations=[
                    create_table_cleaning_operations(event_table.name, event_table_columns),
                    create_table_cleaning_operations(item_table.name, item_table_columns),
                ],
            )
        )
    expected_msg = "Table cleaning operation(s) does not result a new feature version."
    assert expected_msg in str(exc.value)


@pytest.fixture(name="event_metadata")
def event_metadata_fixture(event_table):
    """Event metadata"""
    return {
        "view_mode": "auto",
        "drop_column_names": ["created_at"],
        "column_cleaning_operations": [],
        "table_id": event_table.id,
    }


@pytest.fixture(name="item_metadata")
def item_metadata_fixture(item_table, event_metadata, event_table):
    """Item metadata"""
    return {
        "view_mode": "auto",
        "drop_column_names": [],
        "column_cleaning_operations": [],
        "table_id": item_table.id,
        "event_suffix": "_event_table",
        "event_drop_column_names": event_metadata["drop_column_names"],
        "event_column_cleaning_operations": event_metadata["column_cleaning_operations"],
        "event_table_id": event_table.id,
        "event_join_column_names": ["event_timestamp", "col_int", "cust_id"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table_cleaning_operations, expected_event_metadata, expected_item_metadata",
    [
        # case 1: update item table only
        (
            [create_table_cleaning_operations("sf_item_table", ["event_id_col"])],
            {},  # no change in event metadata
            {
                "column_cleaning_operations": [
                    {
                        "column_name": "event_id_col",
                        "cleaning_operations": [{"type": "missing", "imputed_value": 0}],
                    }
                ],
            },
        ),
        # case 2: update event table only
        (
            [create_table_cleaning_operations("sf_event_table", ["col_int"])],
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
        # case 3: update event table & item table cleaning operations
        (
            [
                create_table_cleaning_operations("sf_event_table", ["col_int"]),
                create_table_cleaning_operations("sf_item_table", ["event_id_col"]),
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
                        "column_name": "event_id_col",
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
async def test_create_new_feature_version__with_item_event_feature(
    version_service,
    feature_item_event,
    event_metadata,
    item_metadata,
    table_cleaning_operations,
    expected_event_metadata,
    expected_item_metadata,
):
    """Test create new feature version with event table cleaning operations"""
    event_view_graph_node = feature_item_event.graph.get_node_by_name("graph_1")
    item_view_graph_node = feature_item_event.graph.get_node_by_name("graph_2")
    compare_pydantic_obj(event_view_graph_node.parameters.metadata, expected=event_metadata)
    compare_pydantic_obj(item_view_graph_node.parameters.metadata, expected=item_metadata)

    # create a new feature version with relevant table cleaning operations
    new_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_item_event.id,
            table_cleaning_operations=table_cleaning_operations,
        )
    )

    # check graph node metadata
    new_event_view_graph_node = new_version.graph.get_node_by_name("graph_1")
    new_item_view_graph_node = new_version.graph.get_node_by_name("graph_2")
    expected_event_metadata = {**event_metadata, **expected_event_metadata}
    expected_item_metadata = {**item_metadata, **expected_item_metadata}
    assert new_event_view_graph_node.parameters.metadata.model_dump() == expected_event_metadata
    assert new_item_view_graph_node.parameters.metadata.model_dump() == expected_item_metadata

    # check consistencies
    event_metadata = new_event_view_graph_node.parameters.metadata
    item_metadata = new_item_view_graph_node.parameters.metadata
    assert (
        item_metadata.event_column_cleaning_operations == event_metadata.column_cleaning_operations
    )
    assert item_metadata.event_drop_column_names == event_metadata.drop_column_names


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings(
    version_service,
    event_table_service,
    feature,
    event_table,
):
    """Test create new feature version using source settings"""
    # check current feature settings
    view_graph_params = feature.graph.get_node_by_name("graph_1").parameters
    assert view_graph_params.metadata.column_cleaning_operations == []

    group_by_params = feature.graph.get_node_by_name("groupby_1").parameters
    assert group_by_params.feature_job_setting == FeatureJobSetting(
        blind_spot="600s", period="1800s", offset="300s"
    )

    # prepare event table before create new version from source settings
    columns_info_with_cdi = []
    for col in event_table.columns_info:
        if col.name == "col_float":
            col.critical_data_info = CriticalDataInfo(
                cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
            )
        columns_info_with_cdi.append(col)

    await event_table_service.update_document(
        document_id=event_table.id,
        data=EventTableServiceUpdate(
            default_feature_job_setting=FeatureJobSetting(
                blind_spot="1h", period="2h", offset="30m"
            ),
            columns_info=columns_info_with_cdi,
        ),
    )

    # create new version from source settings & check the feature job setting & table cleaning operations
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
    assert group_by_params.feature_job_setting == FeatureJobSetting(
        blind_spot="3600s", period="7200s", offset="1800s"
    )


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings_scd_table(
    version_service, snowflake_scd_table_with_entity, arbitrary_default_feature_job_setting
):
    """Test create new feature version using source settings for SCD table"""
    change_view = snowflake_scd_table_with_entity.get_change_view(
        track_changes_column="col_boolean"
    )

    # check the fjs is different from the default fjs
    assert (
        arbitrary_default_feature_job_setting
        != snowflake_scd_table_with_entity.default_feature_job_setting
    )

    feature_group = change_view.groupby("col_text").aggregate_over(
        value_column=None,
        method="count",
        windows=["30d"],
        feature_names=["feat_30d"],
        feature_job_setting=arbitrary_default_feature_job_setting,
    )
    feature = feature_group["feat_30d"]
    feature.save()

    # check feature's table feature job setting
    assert feature.cached_model.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_scd_table_with_entity.id,
            feature_job_setting=arbitrary_default_feature_job_setting,
        )
    ]

    # check create new version using source settings
    new_version = await version_service.create_new_feature_version_using_source_settings(
        document_id=feature.id
    )
    assert new_version.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_scd_table_with_entity.id,
            feature_job_setting=snowflake_scd_table_with_entity.default_feature_job_setting,
        )
    ]


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings_ts_window_agg_feature(
    version_service, snowflake_time_series_table_with_entity, ts_window_aggregate_feature
):
    """Test create new feature version using source settings for SCD table"""
    ts_window_aggregate_feature.save()

    assert ts_window_aggregate_feature.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_time_series_table_with_entity.id,
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 8 1 * *", blind_spot=CalendarWindow(unit="MONTH", size=1)
            ),
        )
    ]

    # update the default feature job setting of the time series table
    snowflake_time_series_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=CronFeatureJobSetting(crontab="0 8 2 * *")
    )

    # check create new version using source settings
    new_version = await version_service.create_new_feature_version_using_source_settings(
        document_id=ts_window_aggregate_feature.id
    )
    assert new_version.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=snowflake_time_series_table_with_entity.id,
            feature_job_setting=CronFeatureJobSetting(
                crontab="0 8 2 * *", reference_timezone="Etc/UTC"
            ),
        )
    ]


@pytest.mark.asyncio
async def test_create_new_feature_version_using_source_settings_ts_window_agg_feature_from_event_table(
    event_table_service,
    version_service,
    snowflake_event_table_with_entity,
    ts_window_aggregate_feature_from_event_table,
):
    """
    Test create new feature version using source settings for time series aggregate from EventTable.

    The feature is a time series window aggregate derived from an EventTable. When the source
    settings are used, the feature job setting should not be replaced because changing the feature
    job setting from CronFeatureJobSetting to FeatureJobSetting is not supported.
    """
    event_table = snowflake_event_table_with_entity
    ts_window_aggregate_feature_from_event_table.save()

    assert ts_window_aggregate_feature_from_event_table.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=event_table.id,
            feature_job_setting=CronFeatureJobSetting(crontab="0 8 1 * *"),
        )
    ]

    # prepare event table before create new version from source settings
    columns_info_with_cdi = []
    for col in event_table.columns_info:
        if col.name == "col_float":
            col.critical_data_info = CriticalDataInfo(
                cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
            )
        columns_info_with_cdi.append(col)

    await event_table_service.update_document(
        document_id=event_table.id,
        data=EventTableServiceUpdate(
            default_feature_job_setting=FeatureJobSetting(
                blind_spot="1h", period="2h", offset="30m"
            ),
            columns_info=columns_info_with_cdi,
        ),
    )

    # check create new version using source settings (FeatureJobSetting and not cron based)
    new_version = await version_service.create_new_feature_version_using_source_settings(
        document_id=ts_window_aggregate_feature_from_event_table.id
    )
    assert new_version.table_id_feature_job_settings == [
        TableIdFeatureJobSetting(
            table_id=event_table.id,
            feature_job_setting=CronFeatureJobSetting(crontab="0 8 1 * *"),
        )
    ]


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
    version_service, event_table_factory, feature_factory
):
    """
    Test that creating a new feature version using source that doesn't have a feature job setting will throw an error.
    """
    await event_table_factory(True)
    feature = await feature_factory()

    with pytest.raises(NoFeatureJobSettingInSourceError) as exc:
        await version_service.create_new_feature_version_using_source_settings(feature.id)
    assert "No feature job setting found in source" in str(exc)


@pytest.mark.asyncio
async def test_feature_and_feature_list_version__catalog_id_used_in_query(
    version_service, feature, feature_list, user, persistent
):
    """Test feature version & feature list version - catalog_id used in query"""
    assert feature.version.suffix is None
    assert feature_list.version.suffix is None

    # create new feature version & new feature list version and check their version suffix
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
                    ),
                )
            ],
        )
    )
    assert new_feat_version.version.suffix == 1

    new_feat_list_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list.id,
            features=[FeatureVersionInfo(name=feature.name, version=new_feat_version.version)],
        )
    )
    assert new_feat_list_version.version.suffix == 1

    # create another feature & feature list with the same name using different catalog ID
    another_catalog_id = ObjectId()
    feature_id_another_catalog = await persistent.insert_one(
        collection_name="feature",
        document={
            "name": feature.name,
            "version": {"name": feature.version.name},
            "catalog_id": another_catalog_id,
        },
        user_id=user.id,
    )
    feat_another_catalog = await persistent.find_one(
        collection_name="feature", query_filter={"_id": feature_id_another_catalog}
    )
    feat_list_id_another_catalog = await persistent.insert_one(
        collection_name="feature_list",
        document={
            "name": feature_list.name,
            "version": {"name": feature_list.version.name},
            "catalog_id": another_catalog_id,
        },
        user_id=user.id,
    )
    feat_list_another_catalog = await persistent.find_one(
        collection_name="feature_list", query_filter={"_id": feat_list_id_another_catalog}
    )
    # make sure version is in correct format
    assert feat_another_catalog["version"] == {"name": feature.version.name}
    assert feat_list_another_catalog["version"] == {"name": feature_list.version.name}

    # check that the version suffix is 2 but not 3
    new_feat_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name="sf_event_table",
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h30s"
                    ),
                )
            ],
        )
    )
    assert new_feat_version.version.suffix == 2

    new_feat_list_version = await version_service.create_new_feature_list_version(
        data=FeatureListNewVersionCreate(
            source_feature_list_id=feature_list.id,
            features=[FeatureVersionInfo(name=feature.name, version=new_feat_version.version)],
        )
    )
    assert new_feat_list_version.version.suffix == 2


@pytest.mark.asyncio
async def test_feature_create_new_version_without_save(app_container, feature, event_table):
    """Test feature create new version without saving the newly created feature"""
    new_feat_version = await app_container.version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=event_table.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
                    ),
                )
            ],
        ),
        to_save=False,
    )
    with pytest.raises(DocumentNotFoundError):
        await app_container.feature_service.get_document(document_id=new_feat_version.id)


@pytest.mark.asyncio
async def test_feature_version(app_container, feature, event_table):
    """Test feature version"""
    assert feature.version.suffix is None

    # create a new version
    facade_service = app_container.feature_facade_service
    new_feat_version = await facade_service.create_new_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=event_table.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="1d", period="1d", offset="1h"
                    ),
                )
            ],
        ),
    )
    assert new_feat_version.version.suffix == 1

    # delete original feature
    await facade_service.delete_feature(feature_id=feature.id)

    # create another version
    another_feat_version = await facade_service.create_new_version(
        data=FeatureNewVersionCreate(
            source_feature_id=new_feat_version.id,
            table_feature_job_settings=[
                TableFeatureJobSetting(
                    table_name=event_table.name,
                    feature_job_setting=FeatureJobSetting(
                        blind_spot="2d", period="1d", offset="1h"
                    ),
                )
            ],
        ),
    )
    assert another_feat_version.version.suffix == 2

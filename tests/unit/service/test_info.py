"""
Test for InfoService
"""
import pytest
from bson import ObjectId

from featurebyte import Entity, SnowflakeDetails
from featurebyte.models.base import DEFAULT_WORKSPACE_ID, PydanticObjectId
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature import FeatureBriefInfo, ReadinessComparison, VersionComparison
from featurebyte.schema.info import (
    DataBriefInfo,
    DataColumnInfo,
    DimensionDataInfo,
    EntityBriefInfo,
    EntityInfo,
    EventDataInfo,
    FeatureInfo,
    FeatureListBriefInfo,
    FeatureListInfo,
    FeatureListNamespaceInfo,
    FeatureNamespaceInfo,
    FeatureStoreInfo,
    ItemDataInfo,
    SCDDataInfo,
)
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.service.info import InfoService


@pytest.fixture(name="info_service")
def info_service_fixture(user, persistent):
    """InfoService fixture"""
    return InfoService(user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID)


@pytest.mark.asyncio
async def test_get_feature_store_info(info_service, feature_store):
    """Test get_feature_sotre_info"""
    info = await info_service.get_feature_store_info(document_id=feature_store.id, verbose=False)
    expected_info = FeatureStoreInfo(
        name="sf_featurestore",
        source="snowflake",
        database_details=SnowflakeDetails(
            account="sf_account",
            warehouse="sf_warehouse",
            database="sf_database",
            sf_schema="sf_schema",
        ),
        created_at=info.created_at,
        updated_at=None,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_store_info(document_id=feature_store.id, verbose=True)
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_entity_info(info_service, entity):
    """Test get_entity_info"""
    info = await info_service.get_entity_info(document_id=entity.id, verbose=False)
    expected_info = EntityInfo(
        name="customer",
        created_at=info.created_at,
        updated_at=None,
        serving_names=["cust_id"],
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_entity_info(document_id=entity.id, verbose=True)
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_event_data_info(info_service, event_data, entity):
    """Test get_event_data_info"""
    info = await info_service.get_event_data_info(document_id=event_data.id, verbose=False)
    expected_info = EventDataInfo(
        name="sf_event_data",
        status="DRAFT",
        event_timestamp_column="event_timestamp",
        event_id_column="col_int",
        record_creation_date_column="created_at",
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_event_table",
        ),
        default_job_setting=None,
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        semantics=["event_timestamp"],
        column_count=9,
        columns_info=None,
        created_at=info.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_event_data_info(document_id=event_data.id, verbose=True)
    assert info == EventDataInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                DataColumnInfo(name="col_int", dtype="INT", entity=entity.name),
                DataColumnInfo(name="col_float", dtype="FLOAT"),
                DataColumnInfo(name="col_char", dtype="CHAR"),
                DataColumnInfo(name="col_text", dtype="VARCHAR"),
                DataColumnInfo(name="col_binary", dtype="BINARY"),
                DataColumnInfo(name="col_boolean", dtype="BOOL"),
                DataColumnInfo(
                    name="event_timestamp", dtype="TIMESTAMP_TZ", semantic="event_timestamp"
                ),
                DataColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="cust_id", dtype="INT", entity=entity.name),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_item_data_info(info_service, item_data, event_data):
    """Test get_item_data_info"""
    _ = event_data
    info = await info_service.get_item_data_info(document_id=item_data.id, verbose=False)
    expected_info = ItemDataInfo(
        name="sf_item_data",
        status="DRAFT",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_data_name="sf_event_data",
        record_creation_date_column=None,
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_item_table",
        ),
        entities=[],
        semantics=[],
        column_count=6,
        columns_info=None,
        created_at=info.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_item_data_info(document_id=item_data.id, verbose=True)
    assert info == ItemDataInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                DataColumnInfo(name="event_id_col", dtype="INT"),
                DataColumnInfo(name="item_id_col", dtype="VARCHAR"),
                DataColumnInfo(name="item_type", dtype="VARCHAR"),
                DataColumnInfo(name="item_amount", dtype="FLOAT"),
                DataColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="event_timestamp", dtype="TIMESTAMP_TZ"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_dimension_data_info(info_service, dimension_data):
    """Test get_dimension_data_info"""
    info = await info_service.get_dimension_data_info(document_id=dimension_data.id, verbose=False)
    expected_info = DimensionDataInfo(
        name="sf_dimension_data",
        status="DRAFT",
        dimension_id_column="col_int",
        record_creation_date_column="created_at",
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_dimension_table",
        ),
        entities=[],
        semantics=[],
        column_count=9,
        columns_info=None,
        created_at=info.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_dimension_data_info(document_id=dimension_data.id, verbose=True)
    assert info == DimensionDataInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                DataColumnInfo(name="col_int", dtype="INT"),
                DataColumnInfo(name="col_float", dtype="FLOAT"),
                DataColumnInfo(name="col_char", dtype="CHAR"),
                DataColumnInfo(name="col_text", dtype="VARCHAR"),
                DataColumnInfo(name="col_binary", dtype="BINARY"),
                DataColumnInfo(name="col_boolean", dtype="BOOL"),
                DataColumnInfo(name="event_timestamp", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="cust_id", dtype="INT"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_scd_data_info(info_service, scd_data):
    """Test get_scd_data_info"""
    info = await info_service.get_scd_data_info(document_id=scd_data.id, verbose=False)
    expected_info = SCDDataInfo(
        name="sf_scd_data",
        status="DRAFT",
        record_creation_date_column=None,
        current_flag_column="is_active",
        natural_key_column="col_text",
        surrogate_key_column="col_int",
        effective_timestamp_column="effective_timestamp",
        end_timestamp_column="end_timestamp",
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_scd_table",
        ),
        entities=[],
        semantics=[],
        column_count=10,
        columns_info=None,
        created_at=info.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_scd_data_info(document_id=scd_data.id, verbose=True)
    assert info == SCDDataInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                DataColumnInfo(name="col_int", dtype="INT"),
                DataColumnInfo(name="col_float", dtype="FLOAT"),
                DataColumnInfo(name="is_active", dtype="BOOL"),
                DataColumnInfo(name="col_text", dtype="VARCHAR"),
                DataColumnInfo(name="col_binary", dtype="BINARY"),
                DataColumnInfo(name="col_boolean", dtype="BOOL"),
                DataColumnInfo(name="effective_timestamp", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="end_timestamp", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                DataColumnInfo(name="cust_id", dtype="INT"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_feature_info(info_service, production_ready_feature, feature_namespace):
    """Test get_feature_info"""
    info = await info_service.get_feature_info(
        document_id=production_ready_feature.id, verbose=False
    )
    expected_metadata = {
        "main_data": {
            "name": "sf_event_data",
            "data_type": "event_data",
            "id": feature_namespace.tabular_data_ids[0],
        },
        "input_columns": {
            "Input0": {"data": "sf_event_data", "column_name": "col_float", "semantic": None}
        },
        "derived_columns": {},
        "aggregations": {
            "F0": {
                "name": "sum_30m",
                "column": "Input0",
                "function": "sum",
                "keys": ["cust_id"],
                "window": "30m",
                "category": None,
                "filter": False,
            }
        },
        "post_aggregation": None,
    }
    expected_info = FeatureInfo(
        name="sum_30m",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        tabular_data=[
            DataBriefInfo(name="sf_event_data", status="DRAFT", workspace_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype="FLOAT",
        default_feature_id=production_ready_feature.id,
        version=VersionComparison(
            this=production_ready_feature.version.to_str(),
            default=production_ready_feature.version.to_str(),
        ),
        readiness=ReadinessComparison(this="PRODUCTION_READY", default="PRODUCTION_READY"),
        metadata=expected_metadata,
        created_at=feature_namespace.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_info(
        document_id=production_ready_feature.id, verbose=True
    )
    assert info == FeatureInfo(
        **{
            **expected_info.dict(),
            "versions_info": [
                FeatureBriefInfo(
                    version=production_ready_feature.version,
                    readiness="PRODUCTION_READY",
                    created_at=production_ready_feature.created_at,
                )
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_feature_info__complex_feature(info_service, feature_iet):
    """Test get_feature_info"""
    info = await info_service.get_feature_info(document_id=feature_iet.id, verbose=False)
    common_agg_parameters = {
        "filter": False,
        "keys": ["cust_id"],
        "category": None,
        "window": "24h",
        "function": "sum",
    }
    expected_metadata = {
        "main_data": {
            "name": "sf_event_data",
            "data_type": "event_data",
            "id": feature_iet.tabular_data_ids[0],
        },
        "input_columns": {
            "Input0": {
                "data": "sf_event_data",
                "column_name": "event_timestamp",
                "semantic": "event_timestamp",
            },
            "Input1": {"data": "sf_event_data", "column_name": "cust_id", "semantic": None},
        },
        "derived_columns": {
            "X0": {
                "name": "a * log(a)",
                "inputs": ["Input0", "Input1"],
                "transforms": [
                    "lag(entity_columns=['cust_id'], offset=1, timestamp_column='event_timestamp')",
                    "date_diff",
                    "timedelta_extract(property='day')",
                    "lag(entity_columns=['cust_id'], offset=1, timestamp_column='event_timestamp')",
                    "date_diff",
                    "timedelta_extract(property='day')",
                    "add(value=0.1)",
                    "log",
                    "mul",
                ],
            },
            "X1": {
                "name": "a",
                "inputs": ["Input0", "Input1"],
                "transforms": [
                    "lag(entity_columns=['cust_id'], offset=1, timestamp_column='event_timestamp')",
                    "date_diff",
                    "timedelta_extract(property='day')",
                ],
            },
        },
        "aggregations": {
            "F0": {"name": "sum(a * log(a))", "column": "X0", **common_agg_parameters},
            "F1": {"name": "sum(a) (24h)", "column": "X1", **common_agg_parameters},
        },
        "post_aggregation": {
            "inputs": ["F0", "F1"],
            "name": "iet_entropy_24h",
            "transforms": ["mul(value=-1)", "div", "add(value=0.1)", "log", "add"],
        },
    }
    expected_info = FeatureInfo(
        name="iet_entropy_24h",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        tabular_data=[
            DataBriefInfo(name="sf_event_data", status="DRAFT", workspace_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype="FLOAT",
        default_feature_id=feature_iet.id,
        version=VersionComparison(
            this=feature_iet.version.to_str(),
            default=feature_iet.version.to_str(),
        ),
        readiness=ReadinessComparison(this="DRAFT", default="DRAFT"),
        metadata=expected_metadata,
        created_at=info.created_at,
        updated_at=info.updated_at,
        workspace_name="default",
    )
    assert info.dict() == expected_info.dict()


@pytest.mark.asyncio
async def test_get_feature_namespace_info(info_service, feature_namespace):
    """Test get_feature_namespace_info"""
    info = await info_service.get_feature_namespace_info(
        document_id=feature_namespace.id, verbose=False
    )
    expected_info = FeatureNamespaceInfo(
        name="sum_30m",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        tabular_data=[
            DataBriefInfo(name="sf_event_data", status="DRAFT", workspace_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype="FLOAT",
        default_feature_id=feature_namespace.default_feature_id,
        created_at=feature_namespace.created_at,
        updated_at=None,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_namespace_info(
        document_id=feature_namespace.id, verbose=True
    )
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_feature_list_info(info_service, feature_list, feature_list_namespace):
    """Test get_feature_list_info"""
    info = await info_service.get_feature_list_info(document_id=feature_list.id, verbose=False)
    expected_info = FeatureListInfo(
        name="sf_feature_list",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        tabular_data=[
            DataBriefInfo(name="sf_event_data", status="DRAFT", workspace_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype_distribution=[{"dtype": "FLOAT", "count": 1}],
        status="DRAFT",
        feature_count=1,
        version=VersionComparison(
            this=feature_list.version.to_str(), default=feature_list.version.to_str()
        ),
        production_ready_fraction={"this": 0.0, "default": 0.0},
        created_at=feature_list_namespace.created_at,
        updated_at=None,
        deployed=False,
        serving_endpoint=None,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_list_info(document_id=feature_list.id, verbose=True)
    assert info == FeatureListInfo(
        **{
            **expected_info.dict(),
            "versions_info": [
                FeatureListBriefInfo(
                    version=feature_list.version,
                    readiness_distribution=[{"readiness": "DRAFT", "count": 1}],
                    created_at=feature_list.created_at,
                    production_ready_fraction=0.0,
                ),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_feature_list_namespace_info(info_service, feature_list_namespace):
    """Test get_feature_list_namespace_info"""
    info = await info_service.get_feature_list_namespace_info(
        document_id=feature_list_namespace.id, verbose=False
    )
    expected_info = FeatureListNamespaceInfo(
        name="sf_feature_list",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], workspace_name="default")
        ],
        tabular_data=[
            DataBriefInfo(name="sf_event_data", status="DRAFT", workspace_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype_distribution=[{"dtype": "FLOAT", "count": 1}],
        default_feature_list_id=feature_list_namespace.default_feature_list_id,
        status="DRAFT",
        feature_count=1,
        created_at=feature_list_namespace.created_at,
        updated_at=None,
        workspace_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_list_namespace_info(
        document_id=feature_list_namespace.id, verbose=True
    )
    assert info == expected_info


def test_get_main_data(info_service, item_data, event_data, dimension_data):
    """Test _get_main_data logic"""
    new_columns_info = []
    for i, col in enumerate(dimension_data.columns_info):
        new_columns_info.append({**col.dict(), "entity_id": ObjectId() if i == 0 else None})
    dimension_data_with_entity = DimensionDataModel(
        **{**dimension_data.dict(), "columns_info": new_columns_info}
    )
    assert (
        info_service._get_main_data(
            [item_data, event_data, dimension_data, dimension_data_with_entity]
        )
        == item_data
    )
    assert (
        info_service._get_main_data([event_data, dimension_data, dimension_data_with_entity])
        == event_data
    )
    assert (
        info_service._get_main_data([dimension_data, dimension_data_with_entity])
        == dimension_data_with_entity
    )
    assert info_service._get_main_data([dimension_data]) == dimension_data


@pytest.fixture(name="transaction_entity")
def entity_fixture():
    """
    Transaction entity fixture
    """
    entity = Entity(name="transaction", serving_names=["transaction_id"])
    entity.save()
    yield entity


@pytest.mark.asyncio
async def test_get_relationship_info_info(
    relationship_info_service, info_service, event_data, entity, transaction_entity
):
    """
    Test get relationship info info
    """
    # create new relationship
    relationship_type = RelationshipType.CHILD_PARENT
    created_relationship = await relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            primary_entity_id=entity.id,
            related_entity_id=transaction_entity.id,
            primary_data_source_id=event_data.id,
            is_enabled=True,
            updated_by=PydanticObjectId(ObjectId()),
        )
    )
    relationship_info = await info_service.get_relationship_info_info(created_relationship.id)
    assert relationship_info.relationship_type == relationship_type
    assert relationship_info.primary_entity_name == "customer"
    assert relationship_info.related_entity_name == "transaction"
    assert relationship_info.data_source_name == "sf_event_data"
    assert relationship_info.updated_by == "default user"

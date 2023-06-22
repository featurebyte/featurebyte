"""
Test for InfoService
"""
import pytest
from bson import ObjectId

from featurebyte import (
    ColumnCleaningOperation,
    Entity,
    FeatureJobSetting,
    MissingValueImputation,
    TableCleaningOperation,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.relationship import RelationshipType
from featurebyte.query_graph.node.schema import SnowflakeDetails, TableDetails
from featurebyte.schema.feature import (
    FeatureBriefInfo,
    FeatureNewVersionCreate,
    ReadinessComparison,
    TableCleaningOperationComparison,
    TableFeatureJobSettingComparison,
    VersionComparison,
)
from featurebyte.schema.info import (
    DimensionTableInfo,
    EntityBriefInfo,
    EntityInfo,
    EventTableInfo,
    FeatureInfo,
    FeatureListBriefInfo,
    FeatureListInfo,
    FeatureListNamespaceInfo,
    FeatureNamespaceInfo,
    FeatureStoreInfo,
    ItemTableInfo,
    SCDTableInfo,
    TableBriefInfo,
    TableColumnInfo,
)
from featurebyte.schema.relationship_info import RelationshipInfoCreate
from featurebyte.schema.target import TargetInfo


@pytest.mark.asyncio
async def test_get_feature_store_info(feature_store_service, feature_store):
    """Test get_feature_store_info"""
    info = await feature_store_service.get_feature_store_info(
        document_id=feature_store.id, verbose=False
    )
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
        catalog_name="default",
    )
    assert info == expected_info

    info = await feature_store_service.get_feature_store_info(
        document_id=feature_store.id, verbose=True
    )
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_entity_info(app_container, entity):
    """Test get_entity_info"""
    info = await app_container.entity_controller.get_info(document_id=entity.id, verbose=False)
    expected_info = EntityInfo(
        name="customer",
        created_at=info.created_at,
        updated_at=None,
        serving_names=["cust_id"],
        catalog_name="default",
    )
    assert info == expected_info

    info = await app_container.entity_controller.get_info(document_id=entity.id, verbose=True)
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_event_table_info(app_container, event_table, entity):
    """Test get_event_table_info"""
    info = await app_container.event_table_controller.get_info(
        document_id=event_table.id, verbose=False
    )
    expected_info = EventTableInfo(
        name="sf_event_table",
        status="PUBLIC_DRAFT",
        event_timestamp_column="event_timestamp",
        event_id_column="col_int",
        record_creation_timestamp_column="created_at",
        table_details=TableDetails(
            database_name="sf_database",
            schema_name="sf_schema",
            table_name="sf_event_table",
        ),
        default_feature_job_setting=FeatureJobSetting(
            blind_spot="10m", frequency="30m", time_modulo_frequency="5m"
        ),
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        semantics=["event_timestamp"],
        column_count=9,
        columns_info=None,
        created_at=info.created_at,
        updated_at=info.updated_at,
        catalog_name="default",
    )
    assert info == expected_info

    info = await app_container.event_table_controller.get_info(
        document_id=event_table.id, verbose=True
    )
    assert info == EventTableInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                TableColumnInfo(name="col_int", dtype="INT"),
                TableColumnInfo(name="col_float", dtype="FLOAT"),
                TableColumnInfo(name="col_char", dtype="CHAR"),
                TableColumnInfo(name="col_text", dtype="VARCHAR"),
                TableColumnInfo(name="col_binary", dtype="BINARY"),
                TableColumnInfo(name="col_boolean", dtype="BOOL"),
                TableColumnInfo(
                    name="event_timestamp", dtype="TIMESTAMP_TZ", semantic="event_timestamp"
                ),
                TableColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="cust_id", dtype="INT", entity=entity.name),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_item_table_info(app_container, item_table, event_table):
    """Test get_item_table_info"""
    _ = event_table
    info = await app_container.item_table_controller.get_info(
        document_id=item_table.id, verbose=False
    )
    expected_info = ItemTableInfo(
        name="sf_item_table",
        status="PUBLIC_DRAFT",
        event_id_column="event_id_col",
        item_id_column="item_id_col",
        event_table_name="sf_event_table",
        record_creation_timestamp_column=None,
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
        catalog_name="default",
    )
    assert info == expected_info

    info = await app_container.item_table_controller.get_info(
        document_id=item_table.id, verbose=True
    )
    assert info == ItemTableInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                TableColumnInfo(name="event_id_col", dtype="INT"),
                TableColumnInfo(name="item_id_col", dtype="VARCHAR"),
                TableColumnInfo(name="item_type", dtype="VARCHAR"),
                TableColumnInfo(name="item_amount", dtype="FLOAT"),
                TableColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="event_timestamp", dtype="TIMESTAMP_TZ"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_dimension_table_info(app_container, dimension_table):
    """Test get_dimension_table_info"""
    info = await app_container.dimension_table_controller.get_info(
        document_id=dimension_table.id, verbose=False
    )
    expected_info = DimensionTableInfo(
        name="sf_dimension_table",
        status="PUBLIC_DRAFT",
        dimension_id_column="col_int",
        record_creation_timestamp_column="created_at",
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
        catalog_name="default",
    )
    assert info == expected_info

    info = await app_container.dimension_table_controller.get_info(
        document_id=dimension_table.id, verbose=True
    )
    assert info == DimensionTableInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                TableColumnInfo(name="col_int", dtype="INT"),
                TableColumnInfo(name="col_float", dtype="FLOAT"),
                TableColumnInfo(name="col_char", dtype="CHAR"),
                TableColumnInfo(name="col_text", dtype="VARCHAR"),
                TableColumnInfo(name="col_binary", dtype="BINARY"),
                TableColumnInfo(name="col_boolean", dtype="BOOL"),
                TableColumnInfo(name="event_timestamp", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="cust_id", dtype="INT"),
            ],
        }
    )


@pytest.mark.asyncio
async def test_get_scd_table_info(app_container, scd_table):
    """Test get_scd_table_info"""
    info = await app_container.scd_table_controller.get_info(
        document_id=scd_table.id, verbose=False
    )
    expected_info = SCDTableInfo(
        name="sf_scd_table",
        status="PUBLIC_DRAFT",
        record_creation_timestamp_column=None,
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
        catalog_name="default",
    )
    assert info == expected_info

    info = await app_container.scd_table_controller.get_info(document_id=scd_table.id, verbose=True)
    assert info == SCDTableInfo(
        **{
            **expected_info.dict(),
            "columns_info": [
                TableColumnInfo(name="col_int", dtype="INT"),
                TableColumnInfo(name="col_float", dtype="FLOAT"),
                TableColumnInfo(name="is_active", dtype="BOOL"),
                TableColumnInfo(name="col_text", dtype="VARCHAR"),
                TableColumnInfo(name="col_binary", dtype="BINARY"),
                TableColumnInfo(name="col_boolean", dtype="BOOL"),
                TableColumnInfo(name="effective_timestamp", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="end_timestamp", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="created_at", dtype="TIMESTAMP_TZ"),
                TableColumnInfo(name="cust_id", dtype="INT"),
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
        "input_columns": {
            "Input0": {"data": "sf_event_table", "column_name": "col_float", "semantic": None}
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
    table_feature_job_setting = [
        {
            "table_name": "sf_event_table",
            "feature_job_setting": {
                "blind_spot": "600s",
                "frequency": "1800s",
                "time_modulo_frequency": "300s",
            },
        }
    ]
    expected_info = FeatureInfo(
        name="sum_30m",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        primary_entity=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        tables=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
        ],
        primary_table=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
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
        table_cleaning_operation=TableCleaningOperationComparison(this=[], default=[]),
        table_feature_job_setting=TableFeatureJobSettingComparison(
            this=table_feature_job_setting, default=table_feature_job_setting
        ),
        metadata=expected_metadata,
        created_at=feature_namespace.created_at,
        updated_at=info.updated_at,
        catalog_name="default",
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


@pytest.fixture(name="expected_feature_iet_info")
def expected_feature_iet_info_fixture(feature_iet):
    """Expected complex feature info"""
    common_agg_parameters = {
        "filter": False,
        "keys": ["cust_id"],
        "category": None,
        "window": "24h",
        "function": "sum",
    }
    expected_metadata = {
        "input_columns": {
            "Input0": {
                "data": "sf_event_table",
                "column_name": "event_timestamp",
                "semantic": "event_timestamp",
            },
            "Input1": {"data": "sf_event_table", "column_name": "cust_id", "semantic": None},
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
    table_feature_job_setting = {
        "table_name": "sf_event_table",
        "feature_job_setting": {
            "blind_spot": "10800s",
            "frequency": "21600s",
            "time_modulo_frequency": "10800s",
        },
    }
    return FeatureInfo(
        name="iet_entropy_24h",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        primary_entity=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        tables=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
        ],
        primary_table=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
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
        table_feature_job_setting={
            "this": [table_feature_job_setting, table_feature_job_setting],
            "default": [table_feature_job_setting, table_feature_job_setting],
        },
        table_cleaning_operation={"this": [], "default": []},
        metadata=expected_metadata,
        created_at=feature_iet.created_at,
        updated_at=feature_iet.updated_at,
        catalog_name="default",
    )


@pytest.mark.flaky(reruns=3)
@pytest.mark.asyncio
async def test_get_feature_info__complex_feature(
    info_service, feature_iet, expected_feature_iet_info
):
    """Test get_feature_info"""
    info = await info_service.get_feature_info(document_id=feature_iet.id, verbose=False)
    expected = {
        **expected_feature_iet_info.dict(),
        "created_at": info.created_at,
        "updated_at": info.updated_at,
    }
    assert info.dict() == expected


@pytest.mark.asyncio
async def test_get_feature_info__complex_feature_with_cdi(
    info_service, feature_iet, expected_feature_iet_info, version_service
):
    """Test get_feature_info"""
    new_version = await version_service.create_new_feature_version(
        data=FeatureNewVersionCreate(
            source_feature_id=feature_iet.id,
            table_cleaning_operations=[
                TableCleaningOperation(
                    table_name="sf_event_table",
                    column_cleaning_operations=[
                        ColumnCleaningOperation(
                            column_name="cust_id",
                            cleaning_operations=[MissingValueImputation(imputed_value=-1)],
                        ),
                    ],
                )
            ],
        )
    )

    info = await info_service.get_feature_info(document_id=new_version.id, verbose=False)
    expected_version = expected_feature_iet_info.version
    expected = {
        **expected_feature_iet_info.dict(),
        "created_at": info.created_at,
        "updated_at": info.updated_at,
        "table_cleaning_operation": {
            "this": [
                {
                    "table_name": "sf_event_table",
                    "column_cleaning_operations": [
                        {
                            "column_name": "cust_id",
                            "cleaning_operations": [{"type": "missing", "imputed_value": -1}],
                        }
                    ],
                }
            ],
            "default": [],
        },
        "version": {**expected_version.dict(), "this": new_version.version.to_str()},
        "version_count": 2,
    }
    assert info.dict() == expected


@pytest.mark.asyncio
async def test_get_feature_namespace_info(info_service, feature_namespace):
    """Test get_feature_namespace_info"""
    info = await info_service.get_feature_namespace_info(
        document_id=feature_namespace.id, verbose=False
    )
    expected_info = FeatureNamespaceInfo(
        name="sum_30m",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        primary_entity=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        tables=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
        ],
        primary_table=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype="FLOAT",
        default_feature_id=feature_namespace.default_feature_id,
        created_at=feature_namespace.created_at,
        updated_at=None,
        catalog_name="default",
    )
    assert info == expected_info

    info = await info_service.get_feature_namespace_info(
        document_id=feature_namespace.id, verbose=True
    )
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_feature_list_info(feature_list_service, feature_list, feature_list_namespace):
    """Test get_feature_list_info"""
    info = await feature_list_service.get_feature_list_info(
        document_id=feature_list.id, verbose=False
    )
    expected_info = FeatureListInfo(
        name="sf_feature_list",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        primary_entity=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        tables=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
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
        default_feature_fraction={"this": 1.0, "default": 1.0},
        created_at=feature_list_namespace.created_at,
        updated_at=None,
        deployed=False,
        serving_endpoint=None,
        catalog_name="default",
        default_feature_list_id=feature_list_namespace.default_feature_list_id,
    )
    assert info == expected_info

    info = await feature_list_service.get_feature_list_info(
        document_id=feature_list.id, verbose=True
    )
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
async def test_get_feature_list_namespace_info(
    feature_list_namespace_service, feature_list_namespace, feature
):
    """Test get_feature_list_namespace_info"""
    info = await feature_list_namespace_service.get_feature_list_namespace_info(
        document_id=feature_list_namespace.id, verbose=False
    )
    expected_info = FeatureListNamespaceInfo(
        name="sf_feature_list",
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        primary_entity=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        tables=[
            TableBriefInfo(name="sf_event_table", status="PUBLIC_DRAFT", catalog_name="default")
        ],
        default_version_mode="AUTO",
        version_count=1,
        dtype_distribution=[{"dtype": "FLOAT", "count": 1}],
        default_feature_list_id=feature_list_namespace.default_feature_list_id,
        feature_namespace_ids=feature_list_namespace.feature_namespace_ids,
        default_feature_ids=[feature.id],
        status="DRAFT",
        feature_count=1,
        created_at=feature_list_namespace.created_at,
        updated_at=None,
        catalog_name="default",
    )
    assert info == expected_info

    info = await feature_list_namespace_service.get_feature_list_namespace_info(
        document_id=feature_list_namespace.id, verbose=True
    )
    assert info == expected_info


@pytest.mark.asyncio
async def test_get_target_info(app_container, entity, target):
    """
    Test get_target_info
    """
    _ = entity
    target_info = await app_container.target_controller.get_info(target.id, verbose=False)

    expected_info = TargetInfo(
        id=target.id,
        target_name=target.name,
        entities=[
            EntityBriefInfo(name="customer", serving_names=["cust_id"], catalog_name="default")
        ],
        horizon=target.horizon,
        blind_spot=target.blind_spot,
        has_recipe=bool(target.graph),
        created_at=target.created_at,
        updated_at=target.updated_at,
    )
    assert target_info == expected_info


@pytest.fixture(name="transaction_entity")
def transaction_entity_fixture():
    """
    Transaction entity fixture
    """
    entity = Entity(name="transaction", serving_names=["transaction_id"])
    entity.save()
    yield entity


@pytest.mark.asyncio
async def test_get_relationship_info_info(app_container, event_table, entity, transaction_entity):
    """
    Test get relationship info info
    """
    # create new relationship
    relationship_type = RelationshipType.CHILD_PARENT
    created_relationship = await app_container.relationship_info_service.create_document(
        RelationshipInfoCreate(
            name="test_relationship",
            relationship_type=relationship_type,
            entity_id=entity.id,
            related_entity_id=transaction_entity.id,
            relation_table_id=event_table.id,
            enabled=True,
            updated_by=PydanticObjectId(ObjectId()),
        )
    )
    relationship_info = await app_container.relationship_info_controller.get_info(
        created_relationship.id
    )
    assert relationship_info.relationship_type == relationship_type
    assert relationship_info.entity_name == "customer"
    assert relationship_info.related_entity_name == "transaction"
    assert relationship_info.table_name == "sf_event_table"
    assert relationship_info.updated_by == "default user"

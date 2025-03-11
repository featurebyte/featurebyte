"""
Test catalog cleanup
"""

import asyncio
import re
from unittest.mock import patch

import pytest

from featurebyte import FunctionParameter, UserDefinedFunction
from featurebyte.models.task import Task
from featurebyte.persistent.audit import get_audit_collection_name
from featurebyte.schema.worker.task.catalog_cleanup import CatalogCleanupTaskPayload


def strip_variable_parts(table_name):
    """
    Strips variable IDs and numbers from the table name.

    Args:
        table_name (str): The original table name containing variable parts.

    Returns:
        str: The fixed part of the table name.
    """
    # Split the table name into parts separated by underscores
    parts = table_name.split("_")
    fixed_parts = []
    for part in parts:
        # Skip parts that are purely numeric
        if part.isdigit():
            continue
        # Skip parts that are hex IDs of a certain length (e.g., 24 characters)
        if re.fullmatch(r"[0-9a-fA-F]{16,}", part):
            continue
        # Otherwise, include the part
        fixed_parts.append(part)
    # Join the fixed parts back together
    return "_".join(fixed_parts)


def test_catalog_specific_model_classes(app_container):
    """Test catalog specific model classes"""
    catalog_cleanup_task = app_container.catalog_cleanup_task
    class_names = [
        _class.__name__ for _, _class in catalog_cleanup_task.catalog_specific_model_class_pairs
    ]

    assert sorted(class_names) == [
        "BatchFeatureTableModel",
        "BatchRequestTableModel",
        "ContextModel",
        "DeploymentModel",
        "EntityModel",
        "FeatureJobSettingAnalysisModel",
        "FeatureListModel",
        "FeatureListNamespaceModel",
        "FeatureMaterializePrerequisite",
        "FeatureMaterializeRun",
        "FeatureModel",
        "FeatureNamespaceModel",
        "FeatureTableCacheMetadataModel",
        "HistoricalFeatureTableModel",
        "OfflineStoreFeatureTableModel",
        "OnlineStoreComputeQueryModel",
        "OnlineStoreTableVersion",
        "PeriodicTask",
        "ProxyTableModel",
        "RelationshipInfoModel",
        "StaticSourceTableModel",
        "SystemMetricsModel",
        "TargetModel",
        "TargetNamespaceModel",
        "TargetTableModel",
        "TileJobLogModel",
        "TileModel",
        "UseCaseModel",
        "WarehouseTableModel",
    ]


@pytest.fixture(name="catalog_with_associated_documents")
def fixture_catalog_with_associated_documents(catalog, historical_feature_table):
    """Catalog with associated documents"""
    _ = historical_feature_table

    # create user defined functions (global & catalog specific)
    func_param = FunctionParameter(name="x", dtype="FLOAT")
    UserDefinedFunction.create(
        name="cos_func",
        sql_function_name="cos",
        function_parameters=[func_param],
        output_dtype="FLOAT",
        is_global=True,
    )

    UserDefinedFunction.create(
        name="cos_func_v2",
        sql_function_name="cos",
        function_parameters=[func_param],
        output_dtype="FLOAT",
        is_global=False,
    )
    return catalog


@pytest.mark.asyncio
@patch("featurebyte.worker.util.task_progress_updater.TaskProgressUpdater.update_progress")
@patch("featurebyte.session.base.BaseSession.drop_table")
async def test_catalog_cleanup(
    mock_drop_table, mock_update_progress, catalog_with_associated_documents, app_container
):
    """Test catalog cleanup"""
    catalog = catalog_with_associated_documents

    # check for table audit documents
    _, matched = await app_container.persistent.find(
        collection_name=get_audit_collection_name("table"),
        query_filter={},
    )
    assert matched == 6  # there are some table audit documents

    # check for drop table calls
    assert mock_drop_table.call_count == 0

    # check for storage documents
    storage_path = app_container.storage.base_path
    catalog_path = storage_path / "catalog" / str(catalog.id)
    assert catalog_path.exists()
    remote_files_before = [entry.name for entry in catalog_path.rglob("*") if entry.is_file()]
    assert remote_files_before == ["feature_clusters.json"]

    # check for user defined functions
    _, matched = await app_container.persistent.find(
        collection_name="user_defined_function",
        query_filter={"catalog_id": catalog.id},
    )
    assert matched == 1

    _, matched = await app_container.persistent.find(
        collection_name=get_audit_collection_name("user_defined_function"),
        query_filter={},
    )
    assert matched == 2

    # check task documents
    _, matched = await app_container.persistent.find(
        collection_name=Task.collection_name(),
        query_filter={"kwargs.catalog_id": str(catalog.id)},
    )
    assert matched == 4

    # check for catalog document
    _, matched = await app_container.persistent.find(
        collection_name="catalog",
        query_filter={"_id": catalog.id},
    )
    assert matched == 1

    # soft delete the catalog & run the cleanup task
    catalog_cleanup_task = app_container.catalog_cleanup_task
    catalog_service = app_container.catalog_service
    await catalog_service.soft_delete_document(document_id=catalog.id)

    # add a delay to ensure the soft delete is completed
    await asyncio.sleep(1)

    # run the cleanup task
    await catalog_cleanup_task.execute(
        payload=CatalogCleanupTaskPayload(
            catalog_id=catalog.id,
            cleanup_threshold_in_days=0,
        )
    )

    # check for drop table calls
    called_table_names = []
    for call_args in mock_drop_table.call_args_list:
        assert call_args.kwargs["schema_name"] == "sf_schema"
        assert call_args.kwargs["database_name"] == "sf_database"
        called_table_names.append(strip_variable_parts(call_args.kwargs["table_name"]))
    assert sorted(called_table_names) == [
        "FEATURE_TABLE_CACHE",
        "HISTORICAL_FEATURE_TABLE",
        "OBSERVATION_TABLE",
        "missing_data_OBSERVATION_TABLE",
    ]

    # check for remote files
    remote_files_after = [entry for entry in catalog_path.rglob("*") if entry.is_file()]
    assert remote_files_after == []

    # check for mongo documents
    for collection_name, _ in catalog_cleanup_task.catalog_specific_model_class_pairs:
        _, matched = await app_container.persistent.find(
            collection_name=collection_name,
            query_filter={"catalog_id": catalog.id},
        )
        assert matched == 0

        # check for audit documents
        _, matched = await app_container.persistent.find(
            collection_name=get_audit_collection_name(collection_name),
            query_filter={},
        )
        assert matched == 0

    # check for user defined functions
    _, matched = await app_container.persistent.find(
        collection_name="user_defined_function",
        query_filter={"catalog_id": catalog.id},
    )
    assert matched == 0

    _, matched = await app_container.persistent.find(
        collection_name=get_audit_collection_name("user_defined_function"),
        query_filter={},
    )
    assert matched == 1  # due to the global user defined function

    # check task documents
    _, matched = await app_container.persistent.find(
        collection_name=Task.collection_name(),
        query_filter={"kwargs.catalog_id": str(catalog.id)},
    )
    assert matched == 0

    # check for catalog document
    _, matched = await app_container.persistent.find(
        collection_name="catalog",
        query_filter={"_id": catalog.id},
    )
    assert matched == 0

    _, matched = await app_container.persistent.find(
        collection_name=get_audit_collection_name("catalog"),
        query_filter={},
    )
    assert matched == 0

    # check for progress updates percentage is increasing
    percent = 0
    for call_args in mock_update_progress.call_args_list:
        assert call_args.args[0] >= percent
        percent = call_args.args[0]

"""
Catalog Cleanup Task Schema
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Type

import featurebyte.models
from featurebyte.exception import DataWarehouseOperationError
from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel, User
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.catalog_cleanup import CatalogCleanupTaskPayload
from featurebyte.service.catalog import AllCatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

DELETED_CATALOG_CLEANUP_THRESHOLD_IN_DAYS = 7


def get_catalog_subclasses_in_module(
    module: Any,
) -> list[Type[FeatureByteCatalogBaseDocumentModel]]:
    """
    Retrieve strict subclasses of FeatureByteCatalogBaseDocumentModel defined in a given module.
    """
    classes = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        # Ensure the class is defined in this module and is a strict subclass of base_class
        if (
            obj.__module__ == module.__name__
            and issubclass(obj, FeatureByteCatalogBaseDocumentModel)
            and hasattr(obj, "collection_name")
        ):
            try:
                obj.collection_name()
                classes.append(obj)
            except AttributeError:
                print(name, obj)
                pass
    return classes


class CatalogCleanupTask(BaseTask[CatalogCleanupTaskPayload]):
    """
    Catalog Cleanup Task
    """

    payload_class = CatalogCleanupTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        all_catalog_service: AllCatalogService,
        persistent: Persistent,
        storage: Storage,
        session_manager_service: SessionManagerService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.task_progress_updater = task_progress_updater
        self.feature_store_service = feature_store_service
        self.all_catalog_service = all_catalog_service
        self.persistent = persistent
        self.storage = storage
        self.session_manager_service = session_manager_service

    @property
    def cleanup_threshold_in_days(self) -> int:
        """
        Deleted catalog cleanup threshold in days

        Returns
        -------
        int
            deleted catalog cleanup threshold in days
        """
        return DELETED_CATALOG_CLEANUP_THRESHOLD_IN_DAYS

    @property
    def model_packages(self) -> list[Any]:
        """
        Model packages to look for catalog specific models

        Returns
        -------
        list[Any]
            list of model packages
        """
        return [featurebyte.models]

    @property
    def remap_model_classes(self) -> dict[str, Type[FeatureByteCatalogBaseDocumentModel]]:
        """
        Remap model classes to cleanup

        Returns
        -------
        dict[str, Type[FeatureByteCatalogBaseDocumentModel]]
            remap model classes
        """
        return {"table": ProxyTableModel}

    @property
    def catalog_specific_model_class_pairs(
        self,
    ) -> list[tuple[str, Type[FeatureByteCatalogBaseDocumentModel]]]:
        """
        Catalog specific model class pairs to cleanup

        Returns
        -------
        list[Type[FeatureByteCatalogBaseDocumentModel]]
            list of catalog specific models
        """
        collection_name_to_model: dict[str, Any] = {}
        for package in self.model_packages:
            for loader, module_name, is_pkg in pkgutil.walk_packages(
                package.__path__, package.__name__ + "."
            ):
                module = importlib.import_module(module_name)
                for _class in get_catalog_subclasses_in_module(module):
                    collection_name_to_model[_class.collection_name()] = _class

        # remap the model classes to override the class used for the collection
        collection_name_to_model.update(self.remap_model_classes)
        return list(collection_name_to_model.items())

    async def get_task_description(self, payload: CatalogCleanupTaskPayload) -> str:
        return "Catalog clean up task"

    async def cleanup_warehouse_tables(
        self, catalog: CatalogModel, warehouse_tables: set[TableDetails]
    ) -> None:
        """Cleanup the warehouse tables associated with the catalog"""
        feature_store = await self.feature_store_service.get_document(
            catalog.default_feature_store_ids[0]
        )
        session = await self.session_manager_service.get_feature_store_session(
            feature_store, user_override=User(id=feature_store.user_id)
        )
        fs_source_info = feature_store.get_source_info()
        for warehouse_table in warehouse_tables:
            try:
                await session.drop_table(
                    table_name=warehouse_table.table_name,
                    schema_name=warehouse_table.schema_name or fs_source_info.schema_name,
                    database_name=warehouse_table.database_name or fs_source_info.database_name,
                )
            except DataWarehouseOperationError:
                pass

    async def cleanup_store_files(self, remote_file_paths: set[Path]) -> None:
        """Cleanup the storage files associated with the catalog"""
        for remote_file_path in remote_file_paths:
            await self.storage.try_delete_if_exists(remote_file_path)

    async def cleanup_catalog(self, catalog: CatalogModel) -> None:
        """Cleanup the catalog & its associated data"""
        query_filter = {"catalog_id": catalog.id}
        for collection_name, model_class in self.catalog_specific_model_class_pairs:
            docs = await self.persistent.get_iterator(
                collection_name=collection_name,
                query_filter=query_filter,
            )

            # extract the warehouse tables and remote file paths of the model collection
            warehouse_tables = set()
            remote_file_paths = set()
            async for doc_dict in docs:
                obj = model_class(**doc_dict)
                assert isinstance(obj, FeatureByteCatalogBaseDocumentModel)
                warehouse_tables.update(obj.warehouse_tables)
                remote_file_paths.update(obj.remote_storage_paths)

            # cleanup the warehouse tables & remote files
            await self.cleanup_warehouse_tables(
                catalog=catalog,
                warehouse_tables=warehouse_tables,
            )
            await self.cleanup_store_files(remote_file_paths)

            # delete the mongo documents
            await self.persistent.delete_many(
                collection_name=model_class.collection_name(),
                query_filter=query_filter,
                user_id=catalog.user_id,
            )

    async def execute(self, payload: CatalogCleanupTaskPayload) -> Any:
        # compute the cutoff time
        cutoff_time = datetime.now() - timedelta(days=self.cleanup_threshold_in_days)
        with self.all_catalog_service.allow_use_raw_query_filter():
            async for catalog in self.all_catalog_service.list_documents_iterator(
                query_filter={
                    "is_deleted": True,
                    "updated_at": {"$lt": cutoff_time},
                },
                use_raw_query_filter=True,
            ):
                await self.cleanup_catalog(catalog)

"""
Catalog Cleanup Task Schema
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import Any, Type

import featurebyte.models
from featurebyte.common.env_util import is_development_mode
from featurebyte.common.progress import (
    ProgressCallbackType,
    get_ranged_progress_callback,
    ranged_progress_callback_iterator,
)
from featurebyte.exception import DataWarehouseOperationError
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteCatalogBaseDocumentModel
from featurebyte.models.catalog import CatalogModel
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.models.task import Task
from featurebyte.models.user_defined_function import UserDefinedFunctionModel
from featurebyte.persistent import Persistent
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.worker.task.catalog_cleanup import CatalogCleanupTaskPayload
from featurebyte.service.catalog import AllCatalogService
from featurebyte.service.session_helper import SessionHelper
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


def get_catalog_subclasses_in_module(
    module: Any,
) -> list[Type[FeatureByteCatalogBaseDocumentModel]]:
    """
    Retrieve strict subclasses of FeatureByteCatalogBaseDocumentModel defined in a given module.

    Parameters
    ----------
    module : Any
        module to search for subclasses

    Returns
    -------
    list[Type[FeatureByteCatalogBaseDocumentModel]]
        list of subclasses of FeatureByteCatalogBaseDocumentModel
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
                logger.info("Skip extracting collection name for class %s", obj)

    return classes


class CatalogCleanupTask(BaseTask[CatalogCleanupTaskPayload]):
    """
    Catalog Cleanup Task
    """

    payload_class = CatalogCleanupTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        all_catalog_service: AllCatalogService,
        persistent: Persistent,
        storage: Storage,
        session_helper: SessionHelper,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.task_progress_updater = task_progress_updater
        self.all_catalog_service = all_catalog_service
        self.persistent = persistent
        self.storage = storage
        self.session_helper = session_helper

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

    @cached_property
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

    async def _cleanup_warehouse_tables(
        self, catalog: CatalogModel, warehouse_tables: set[TableDetails]
    ) -> None:
        if catalog.default_feature_store_ids:
            fs_and_session = await self.session_helper.try_to_get_feature_store_and_session(
                feature_store_id=catalog.default_feature_store_ids[0]
            )
            if not fs_and_session:
                return

            feature_store, session = fs_and_session
            fs_source_info = feature_store.get_source_info()
            for warehouse_table in warehouse_tables:
                try:
                    await session.drop_table(
                        table_name=warehouse_table.table_name,
                        schema_name=warehouse_table.schema_name or fs_source_info.schema_name,
                        database_name=warehouse_table.database_name or fs_source_info.database_name,
                    )
                except DataWarehouseOperationError as exc:
                    logger.exception(f"Error dropping warehouse table ({warehouse_table}): {exc}")

    async def _cleanup_store_files(self, remote_file_paths: set[Path]) -> None:
        for remote_file_path in remote_file_paths:
            await self.storage.try_delete_if_exists(remote_file_path)

    async def _pre_cleanup_non_catalog_mongo_asset(
        self,
        catalog: CatalogModel,
        progress_callback: ProgressCallbackType,
    ) -> None:
        """
        Handle the cleanup of non-catalog mongo assets before the catalog specific assets are cleaned up.
        This is also used by those assets that is catalog specific but it does not have a corresponding
        catalog ID in the document.

        Parameters
        ----------
        catalog: CatalogModel
            Catalog model object
        progress_callback: ProgressCallbackType
            Progress callback
        """

    async def _post_cleanup_non_catalog_mongo_asset(
        self,
        catalog: CatalogModel,
        progress_callback: ProgressCallbackType,
    ) -> None:
        # cleanup user defined functions
        await progress_callback(
            percent=30,
            message="Cleaning up user defined functions",
        )

        await self.persistent.perma_delete(
            collection_name=UserDefinedFunctionModel.collection_name(),
            query_filter={"catalog_id": catalog.id},
            with_audit=True,
        )

        # cleanup task documents
        await progress_callback(
            percent=60,
            message="Cleaning up task documents",
        )
        await self.persistent.delete_many(
            collection_name=Task.collection_name(),
            query_filter={"kwargs.catalog_id": str(catalog.id)},
            user_id=catalog.user_id,
            disable_audit=True,
        )

        # cleanup the catalog document
        await progress_callback(
            percent=100,
            message="Removing catalog document",
        )
        await self.persistent.perma_delete(
            collection_name=CatalogModel.collection_name(),
            query_filter={"_id": catalog.id},
            with_audit=True,
        )

    async def _cleanup_catalog(
        self, catalog: CatalogModel, progress_callback: ProgressCallbackType
    ) -> None:
        pre_cleanup_percent = 10
        catalog_collection_percent = 90

        # cleanup non-catalog mongo assets
        await self._pre_cleanup_non_catalog_mongo_asset(
            catalog=catalog,
            progress_callback=get_ranged_progress_callback(
                progress_callback=progress_callback,
                from_percent=0,
                to_percent=pre_cleanup_percent,
            ),
        )

        # cleanup catalog specific mongo assets
        query_filter = {"catalog_id": catalog.id}
        mongo_progress_callback = get_ranged_progress_callback(
            progress_callback=progress_callback,
            from_percent=pre_cleanup_percent,
            to_percent=catalog_collection_percent,
        )
        for i, (collection_name, model_class) in enumerate(self.catalog_specific_model_class_pairs):
            await mongo_progress_callback(
                percent=int(100 * i / len(self.catalog_specific_model_class_pairs)),
                message=f"Cleaning up: {collection_name}",
            )
            docs = await self.persistent.get_iterator(
                collection_name=collection_name,
                query_filter=query_filter,
            )

            # extract the warehouse tables and remote file paths of the model collection
            warehouse_tables = set()
            remote_file_paths = set()
            async for doc_dict in docs:
                try:
                    obj = model_class(**doc_dict)
                    assert isinstance(obj, FeatureByteCatalogBaseDocumentModel)
                    warehouse_tables.update(obj.warehouse_tables)
                    remote_file_paths.update(obj.remote_storage_paths)
                except Exception as exc:
                    if is_development_mode():
                        raise

                    # failed to parse the document, probably due to schema changes
                    logger.info(
                        "Failed to parse document %s (%s): %s",
                        collection_name,
                        doc_dict["_id"],
                        exc,
                    )

            # cleanup the warehouse tables & remote files
            await mongo_progress_callback(
                percent=int(100 * i / len(self.catalog_specific_model_class_pairs)),
                message="Cleaning up warehouse tables & remote files",
            )
            await self._cleanup_warehouse_tables(
                catalog=catalog,
                warehouse_tables=warehouse_tables,
            )
            await self._cleanup_store_files(remote_file_paths)

            # delete the mongo documents
            await mongo_progress_callback(
                percent=int(100 * i / len(self.catalog_specific_model_class_pairs)),
                message="Cleaning up Mongo records",
            )
            with_audit = True
            model_settings = getattr(model_class, "Settings", None)
            if model_settings:
                with_audit = getattr(model_settings, "auditable", True)
            await self.persistent.perma_delete(
                collection_name=collection_name,
                query_filter=query_filter,
                with_audit=with_audit,
            )

        # cleanup non-catalog mongo assets
        await self._post_cleanup_non_catalog_mongo_asset(
            catalog=catalog,
            progress_callback=get_ranged_progress_callback(
                progress_callback=progress_callback,
                from_percent=catalog_collection_percent,
                to_percent=100,
            ),
        )

    async def execute(self, payload: CatalogCleanupTaskPayload) -> Any:
        # compute the cutoff time
        cutoff_time = datetime.utcnow() - timedelta(days=payload.cleanup_threshold_in_days)
        with self.all_catalog_service.allow_use_raw_query_filter():
            catalogs_to_cleanup = [
                catalog
                async for catalog in self.all_catalog_service.list_documents_iterator(
                    query_filter={
                        "is_deleted": True,
                        "updated_at": {"$lt": cutoff_time},
                    },
                    use_raw_query_filter=True,
                )
            ]
            for catalog, catalog_progress_callback in ranged_progress_callback_iterator(
                items=catalogs_to_cleanup,
                progress_callback=self.task_progress_updater.update_progress,
            ):
                await catalog_progress_callback(
                    percent=0,
                    message=f'Cleaning up catalog "{catalog.name}"',
                )
                try:
                    await self._cleanup_catalog(
                        catalog, progress_callback=catalog_progress_callback
                    )
                except Exception as exc:
                    logger.exception(f"Error cleaning up catalog ({catalog}): {exc}")
                    if is_development_mode():
                        raise

                    await catalog_progress_callback(
                        percent=100,
                        message=f"Error cleaning up catalog: {exc}",
                    )

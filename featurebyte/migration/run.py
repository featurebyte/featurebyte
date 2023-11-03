"""
Migration script
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Callable, Set, cast

import asyncio
import importlib
import inspect

from celery import Celery

from featurebyte.common.path_util import import_submodules
from featurebyte.logging import get_logger
from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.migration.model import MigrationMetadata, SchemaMetadataModel, SchemaMetadataUpdate
from featurebyte.migration.service import MigrationInfo
from featurebyte.migration.service.mixin import (
    BaseMigrationServiceMixin,
    BaseMongoCollectionMigration,
    DataWarehouseMigrationMixin,
)
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.persistent.base import Persistent
from featurebyte.persistent.mongo import MongoDB
from featurebyte.routes.app_container_config import _get_class_name
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.service.catalog import AllCatalogService
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.worker import get_celery

logger = get_logger(__name__)


def _extract_migrate_method_marker(migrate_method: Any) -> MigrationInfo:
    # extract migrate_method marker from the given migrate_method
    marker = getattr(migrate_method, "_MigrationInfo__marker")
    return cast(MigrationInfo, marker)


def _has_migrate_method_marker(method: Any) -> bool:
    # check whether the given method has migrate_method marker
    return hasattr(method, "_MigrationInfo__marker")


def _extract_migrate_methods(service_class: Any) -> list[tuple[int, str]]:
    # extract all the migrate_methods from a given service_class
    output = []
    for attr_name in dir(service_class):
        attr = getattr(service_class, attr_name)
        if _has_migrate_method_marker(attr):
            marker = _extract_migrate_method_marker(attr)
            output.append((marker.version, attr_name))
    return output


def retrieve_all_migration_methods(data_warehouse_migrations_only: bool = False) -> dict[int, Any]:
    """
    List all the migration methods

    Parameters
    ----------
    data_warehouse_migrations_only: bool
        If True, include data warehouse migrations only

    Returns
    -------
    dict[int, Any]
        Migration version to method data mapping

    Raises
    ------
    ValueError
        When duplicated version is detected
    """
    # import migration service first so that submodules can be imported properly
    migration_service_dir = f"{__name__.rsplit('.', 1)[0]}.service"
    importlib.import_module(migration_service_dir)

    migrate_methods = {}
    for mod in import_submodules(migration_service_dir).values():
        for attr_name in dir(mod):
            attr = getattr(mod, attr_name)
            if inspect.isclass(attr) and issubclass(attr, BaseMigrationServiceMixin):
                if data_warehouse_migrations_only and not issubclass(
                    attr, DataWarehouseMigrationMixin
                ):
                    continue
                for version, migrate_method_name in _extract_migrate_methods(attr):
                    migrate_method_data = {
                        "module": attr.__module__,
                        "class": attr_name,
                        "method": migrate_method_name,
                    }
                    if version not in migrate_methods:
                        migrate_methods[version] = migrate_method_data
                    elif migrate_methods[version] != migrate_method_data:
                        raise ValueError(
                            f"Duplicated migrate version detected between "
                            f"{migrate_methods[version]} and {migrate_method_data}"
                        )
    return migrate_methods


async def catalog_specific_migration_method_constructor(
    all_catalog_service: AllCatalogService,
    user: Any,
    persistent: Persistent,
    migrate_service_instance_name: str,
    migrate_method_name: str,
    migration_marker: MigrationInfo,
    max_concurrency: int = 10,
) -> Callable[[], Any]:
    """
    Decorator for catalog specific migration method. This decorator will loop through all the catalogs
    and run the migration method for each catalog.

    Parameters
    ----------
    all_catalog_service: AllCatalogService
        Catalog service to retrieve all the catalogs
    user: Any
        User object
    persistent: Persistent
        Persistent storage object
    migrate_service_instance_name: str
        Migrate service instance name
    migrate_method_name: str
        Migrate method name
    migration_marker: MigrationInfo
        Migration marker
    max_concurrency: int
        Maximum number of concurrent tasks

    Returns
    -------
    Callable[[], Any]
    """

    async def decorated_migrate_method() -> None:
        tasks: Set[Any] = set()

        # use raw query filter to retrieve all the catalogs (including deleted ones)
        with all_catalog_service.allow_use_raw_query_filter():
            catalog_ids = {
                catalog.id
                async for catalog in all_catalog_service.list_documents_iterator(
                    query_filter={}, use_raw_query_filter=True
                )
            }

        for catalog_id in catalog_ids:
            logger.info(f"Run migration for catalog {catalog_id}")

            # construct the app container for the catalog & retrieve the migrate method
            instance_map = {
                "user": user,
                "persistent": persistent,
                "catalog_id": catalog_id,
            }
            app_container = LazyAppContainer(
                app_container_config=app_container_config,
                instance_map=instance_map,
            )
            migrate_service = app_container.get(migrate_service_instance_name)
            migrate_method = getattr(migrate_service, migrate_method_name)

            if len(tasks) >= max_concurrency:
                # wait for one of the tasks to finish first
                _, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            # add the task to the task list
            tasks.add(asyncio.create_task(migrate_method()))

        # wait for all the tasks to finish
        if tasks:
            done, pending = await asyncio.wait(tasks)
            # make sure all the tasks are done
            assert pending == set()

            # check whether any of the tasks failed
            for task in done:
                task.result()

    return migration_marker(decorated_migrate_method)


async def migrate_method_generator(
    user: Any,
    persistent: Persistent,
    get_credential: Any,
    celery: Celery,
    schema_metadata: SchemaMetadataModel,
    include_data_warehouse_migrations: bool,
) -> AsyncGenerator[tuple[BaseMigrationServiceMixin, Callable[..., Any]], None]:
    """
    Migrate method generator

    Parameters
    ----------
    user: Any
        User object contains id information
    persistent: Persistent
        Persistent storage object
    get_credential: Any
        Callback to retrieve credential
    celery: Celery
        Celery object
    schema_metadata: SchemaMetadataModel
        Schema metadata
    include_data_warehouse_migrations: bool
        Whether to include data warehouse migrations

    Yields
    ------
    migrate_service
        Service to be migrated
    migrate_method
        Migration method
    """
    instance_map = {
        "user": user,
        "persistent": persistent,
        "catalog_id": DEFAULT_CATALOG_ID,
    }
    app_container = LazyAppContainer(
        app_container_config=app_container_config,
        instance_map=instance_map,
    )
    migrate_methods = retrieve_all_migration_methods()
    version_start = schema_metadata.version + 1
    for version in range(version_start, len(migrate_methods) + 1):
        migrate_method_data = migrate_methods[version]
        module = importlib.import_module(migrate_method_data["module"])
        migrate_service_class = getattr(module, migrate_method_data["class"])
        migrate_service_instance_name = _get_class_name(migrate_service_class.__name__)
        migrate_service = app_container.get(migrate_service_instance_name)
        if isinstance(migrate_service, DataWarehouseMigrationMixin):
            if not include_data_warehouse_migrations:
                continue
            migrate_service.set_credential_callback(get_credential)
            migrate_service.set_celery(celery)

        migrate_method_name = migrate_method_data["method"]
        migrate_method = getattr(migrate_service, migrate_method_name)
        if isinstance(migrate_service, BaseMongoCollectionMigration):
            if migrate_service.is_catalog_specific:
                migrate_method = await catalog_specific_migration_method_constructor(
                    all_catalog_service=app_container.all_catalog_service,
                    user=user,
                    persistent=persistent,
                    migrate_service_instance_name=migrate_service_instance_name,
                    migrate_method_name=migrate_method_name,
                    migration_marker=_extract_migrate_method_marker(migrate_method),
                )

        yield migrate_service, migrate_method


async def post_migration_sanity_check(service: BaseMigrationServiceMixin) -> None:
    """
    Post migration sanity check

    Parameters
    ----------
    service: BaseMigrationServiceMixin
        Service used to perform the sanity check
    """
    # check document deserialization
    docs = await service.delegate_service.list_documents_as_dict(page_size=0)
    step_size = max(len(docs["data"]) // 5, 1)
    audit_record_count = 0
    for i, doc_dict in enumerate(docs["data"]):
        document = service.delegate_service.document_class(**doc_dict)

        # check audit records
        if i % step_size == 0:
            async for _ in service.delegate_service.historical_document_generator(
                document_id=document.id
            ):
                audit_record_count += 1

    logger.info(
        f"Successfully loaded {len(docs['data'])} records & {audit_record_count} audit records."
    )


async def run_migration(
    user: Any,
    persistent: Persistent,
    get_credential: Any,
    celery: Celery,
    include_data_warehouse_migrations: bool = True,
) -> None:
    """
    Run database migration

    Parameters
    ----------
    user: Any
        User object
    persistent: Persistent
        Persistent object
    get_credential: Any
        Callback to retrieve credential
    celery: Celery
        Celery object
    include_data_warehouse_migrations: bool
        Whether to include data warehouse migrations
    """
    schema_metadata_service = SchemaMetadataService(
        user=user,
        persistent=persistent,
        catalog_id=DEFAULT_CATALOG_ID,
        block_modification_handler=BlockModificationHandler(),
    )
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA
    )
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
        celery=celery,
        schema_metadata=schema_metadata,
        include_data_warehouse_migrations=include_data_warehouse_migrations,
    )
    async for service, migrate_method in method_generator:
        marker = _extract_migrate_method_marker(migrate_method)
        logger.info(f"Run migration (version={marker.version}): {marker.description}")
        await migrate_method()

        # perform post migration sanity check
        logger.info("Perform post migration sanity check...")
        await post_migration_sanity_check(service)

        # update schema version after migration
        await schema_metadata_service.update_document(
            document_id=schema_metadata.id,
            data=SchemaMetadataUpdate(version=marker.version, description=marker.description),
        )


async def run_mongo_migration(persistent: MongoDB) -> None:
    """
    Run Mongo migration script

    Parameters
    ----------
    persistent: MongoDB
        Mongo persistent object
    """
    credential_provider = MongoBackedCredentialProvider(persistent=persistent)
    await run_migration(
        User(),
        persistent,
        credential_provider.get_credential,
        celery=get_celery(),
        include_data_warehouse_migrations=False,
    )

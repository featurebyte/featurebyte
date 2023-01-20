"""
Migration script
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Callable

import importlib

from featurebyte.app import User
from featurebyte.logger import logger
from featurebyte.migration.helper import (
    _extract_migrate_method_marker,
    retrieve_all_migration_methods,
)
from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.migration.model import MigrationMetadata, SchemaMetadataModel, SchemaMetadataUpdate
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.persistent.base import Persistent
from featurebyte.persistent.mongo import MongoDB
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.utils.credential import get_credential as get_credential_from_config

BaseDocumentServiceT = BaseDocumentService[
    FeatureByteBaseDocumentModel, FeatureByteBaseModel, BaseDocumentServiceUpdateSchema
]


async def migrate_method_generator(
    user: Any,
    persistent: Persistent,
    get_credential: Any,
    schema_metadata: SchemaMetadataModel,
    include_data_warehouse_migrations: bool,
) -> AsyncGenerator[tuple[BaseDocumentServiceT, Callable[..., Any]], None]:
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
    migrate_methods = retrieve_all_migration_methods()
    version_start = schema_metadata.version + 1
    for version in range(version_start, len(migrate_methods) + 1):
        migrate_method_data = migrate_methods[version]
        module = importlib.import_module(migrate_method_data["module"])
        migrate_service_class = getattr(module, migrate_method_data["class"])
        migrate_service = migrate_service_class(user=user, persistent=persistent)
        if isinstance(migrate_service, DataWarehouseMigrationMixin):
            if not include_data_warehouse_migrations:
                continue
            migrate_service.set_credential_callback(get_credential)
        migrate_method = getattr(migrate_service, migrate_method_data["method"])
        yield migrate_service, migrate_method


async def post_migration_sanity_check(service: BaseDocumentServiceT) -> None:
    """
    Post migration sanity check

    Parameters
    ----------
    service: BaseDocumentServiceT
        Service used to perform the sanity check
    """
    # check document deserialization
    docs = await service.list_documents(page_size=0)
    step_size = max(len(docs["data"]) // 5, 1)
    audit_record_count = 0
    for i, doc_dict in enumerate(docs["data"]):
        document = service.document_class(**doc_dict)

        # check audit records
        if i % step_size == 0:
            async for _ in service.historical_document_generator(document_id=document.id):
                audit_record_count += 1

    logger.info(
        f"Successfully loaded {len(docs['data'])} records & {audit_record_count} audit records."
    )


async def run_migration(
    user: Any,
    persistent: Persistent,
    get_credential: Any,
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
    include_data_warehouse_migrations: bool
        Whether to include data warehouse migrations
    """
    schema_metadata_service = SchemaMetadataService(user=user, persistent=persistent)
    schema_metadata = await schema_metadata_service.get_or_create_document(
        name=MigrationMetadata.SCHEMA_METADATA
    )
    method_generator = migrate_method_generator(
        user=user,
        persistent=persistent,
        get_credential=get_credential,
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
    await run_migration(
        User(), persistent, get_credential_from_config, include_data_warehouse_migrations=False
    )

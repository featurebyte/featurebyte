"""
Migration script
"""
from __future__ import annotations

from typing import Any, AsyncGenerator, Callable, Iterator, cast

import importlib
import inspect

from featurebyte.common.path_util import import_submodules
from featurebyte.logger import logger
from featurebyte.migration.migration_data_service import SchemaMetadataService
from featurebyte.migration.service import MigrationInfo
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.persistent.base import Persistent
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService

BaseDocumentServiceT = BaseDocumentService[
    FeatureByteBaseDocumentModel, FeatureByteBaseModel, BaseDocumentServiceUpdateSchema
]


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


def retrieve_all_migration_methods() -> dict[int, Any]:
    """
    List all the migration methods

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
            if inspect.isclass(attr) and issubclass(attr, BaseDocumentService):
                for version, migrate_method_name in _extract_migrate_methods(attr):
                    migrate_method_data = {
                        "module": mod.__name__,
                        "class": attr_name,
                        "method": migrate_method_name,
                    }
                    if version not in migrate_methods:
                        migrate_methods[version] = migrate_method_data
                    else:
                        raise ValueError(
                            f"Duplicated migrate version detected between "
                            f"{migrate_methods[version]} and {migrate_method_data}"
                        )
    return migrate_methods


async def migrate_method_generator(
    user: Any, persistent: Persistent
) -> AsyncGenerator[tuple[BaseDocumentServiceT, Callable[..., Any]], None]:
    """
    Migrate method generator

    Parameters
    ----------
    user: Any
        User object contains id information
    persistent: Persistent
        Persistent storage object

    Yields
    ------
    migrate_service
        Service to be migrated
    migrate_method
        Migration method
    """
    schema_metadata_service = SchemaMetadataService(user=user, persistent=persistent)
    schema_metadata = await schema_metadata_service.get_or_create_document()
    migrate_methods = retrieve_all_migration_methods()

    version_start = schema_metadata.version + 1
    for version in range(version_start, len(migrate_methods) + 1):
        migrate_method_data = migrate_methods[version]
        module = importlib.import_module(migrate_method_data["module"])
        migrate_service_class = getattr(module, migrate_method_data["class"])
        migrate_service = migrate_service_class(user=user, persistent=persistent)
        migrate_method = getattr(migrate_service, migrate_method_data["method"])
        yield migrate_service, migrate_method


def document_historical_travel_generator(
    audit_docs: list[dict[str, Any]]
) -> Iterator[dict[str, Any]]:
    """
    Reconstruct documents of older history

    Parameters
    ----------
    audit_docs: list[dict[str, Any]]
        Audit records

    Yields
    -------
    Iterator[dict[str, Any]]
        Iterator of older historical records
    """
    doc = {}
    for audit_doc in sorted(audit_docs, key=lambda record: (record["action_at"], record["_id"])):
        previous_values = audit_doc["previous_values"]
        current_values = audit_doc["current_values"]
        if audit_doc["action_type"] == "INSERT":
            doc = current_values
            doc["_id"] = audit_doc["document_id"]
        else:
            doc = {
                key: current_values.get(key, doc.get(key))
                for key in set(current_values).union(set(doc).difference(previous_values))
            }
        yield doc


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
    for i, doc_dict in enumerate(docs["data"]):
        document = service.document_class(**doc_dict)

        # check audit records
        if i % step_size == 0:
            audit_docs = await service.list_document_audits(document_id=document.id, page_size=0)
            for hist_doc_dict in document_historical_travel_generator(audit_docs["data"]):
                _ = service.document_class(**hist_doc_dict)


async def run_migration(user: Any, persistent: Persistent) -> None:
    """
    Run database migration

    Parameters
    ----------
    user: Any
        User object
    persistent: Persistent
        Persistent object
    """
    method_generator = migrate_method_generator(user=user, persistent=persistent)
    async for service, migrate_method in method_generator:
        marker = _extract_migrate_method_marker(migrate_method)
        logger.info(f"Run migration (version={marker.version}): {marker.description}")
        await migrate_method()

        # perform post migration sanity check
        logger.info("Perform post migration sanity check...")
        await post_migration_sanity_check(service)

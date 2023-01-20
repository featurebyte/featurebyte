"""
Migration related helpers
"""
from __future__ import annotations

from typing import Any, cast

import importlib
import inspect

from featurebyte.common.path_util import import_submodules
from featurebyte.migration.service import MigrationInfo
from featurebyte.migration.service.mixin import DataWarehouseMigrationMixin
from featurebyte.service.base_document import BaseDocumentService


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
            if inspect.isclass(attr) and issubclass(attr, BaseDocumentService):
                if data_warehouse_migrations_only and not issubclass(
                    attr, DataWarehouseMigrationMixin
                ):
                    continue
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

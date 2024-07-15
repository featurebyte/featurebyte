"""
Database Migration
"""

import functools
from typing import Any, Awaitable, Callable, TypeVar, cast

from pydantic import BaseModel

AwaitableMigrateFunction = TypeVar(
    "AwaitableMigrateFunction", bound=Callable[..., Awaitable[object]]
)


class MigrationInfo(BaseModel):
    """MigrationInfo class"""

    version: int
    description: str

    def _decorate_migrate(self, function: AwaitableMigrateFunction) -> AwaitableMigrateFunction:
        @functools.wraps(function)
        async def _wrap_migrate(*args: Any, **kwargs: Any) -> Any:
            return await function(*args, **kwargs)

        _wrap_migrate.__marker = self  # type: ignore[attr-defined]
        return cast(AwaitableMigrateFunction, _wrap_migrate)

    def __call__(self, function: AwaitableMigrateFunction) -> AwaitableMigrateFunction:
        return self._decorate_migrate(function)


def migrate(
    version: int, description: str
) -> Callable[[AwaitableMigrateFunction], AwaitableMigrateFunction]:
    """
    Migrate decorator to add migration info into decorated function

    Parameters
    ----------
    version: int
        Migration version number
    description: str
        Migration description

    Returns
    -------
    Function decorated with migration info
    """
    migration_marker = MigrationInfo(version=version, description=description)
    return migration_marker

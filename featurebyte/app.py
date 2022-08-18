"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Callable

import signal

from bson.objectid import ObjectId
from fastapi import Depends, FastAPI, Request

import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_list.api as feature_list_api
import featurebyte.routes.feature_namespace.api as feature_namespace_api
import featurebyte.routes.feature_store.api as feature_store_api
import featurebyte.routes.task_status.api as task_status_api
from featurebyte.config import Configurations
from featurebyte.models.credential import Credential
from featurebyte.persistent import GitDB, Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_data.controller import EventDataController
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.feature_store.controller import FeatureStoreController
from featurebyte.routes.task_status.controller import TaskStatusController

app = FastAPI()
PERSISTENT = None


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    id = None


def _get_persistent() -> Persistent:
    """
    Return global Persistent object

    Returns
    -------
    Persistent
        Persistent object

    Raises
    ------
    ValueError
        Git configurations not available
    """
    global PERSISTENT  # pylint: disable=global-statement
    if not PERSISTENT:
        config = Configurations()
        if not config.git:
            raise ValueError("Git settings not available in configurations")
        git_db = GitDB(**config.git.dict())
        PERSISTENT = git_db
    return PERSISTENT


async def _get_credential(user_id: ObjectId | None, feature_store_name: str) -> Credential | None:
    """
    Retrieve credential from FeatureStoreModel

    Parameters
    ----------
    user_id: ObjectId | None
        User ID
    feature_store_name: str
        FeatureStore name

    Returns
    -------
    Credential
        Credential for the database source
    """
    _ = user_id
    config = Configurations()
    return config.credentials.get(feature_store_name)


def _get_api_deps(controller: type) -> Callable[[Request], None]:
    """
    Get API dependency injection function

    Parameters
    ----------
    controller: type
        Controller class

    Returns
    -------
    Callable[Request]
        Dependency injection function
    """

    def _dep_injection_func(request: Request) -> None:
        """
        Inject dependencies into the requests

        Parameters
        ----------
        request: Request
            Request object to be updated
        """
        request.state.persistent = _get_persistent()
        request.state.user = User()
        request.state.get_credential = _get_credential
        request.state.controller = controller

    return _dep_injection_func


# add routers into the app
resource_api_controller_pairs: list[tuple[Any, type[BaseController[Any, Any]]]] = [
    (event_data_api, EventDataController),
    (entity_api, EntityController),
    (feature_api, FeatureController),
    (feature_list_api, FeatureListController),
    (feature_namespace_api, FeatureNamespaceController),
    (feature_store_api, FeatureStoreController),
    (task_status_api, TaskStatusController),
]
for resource_api, resource_controller in resource_api_controller_pairs:
    tags = []
    if hasattr(resource_controller, "collection_name"):
        tags = [resource_controller.collection_name]

    app.include_router(
        resource_api.router,
        dependencies=[Depends(_get_api_deps(resource_controller))],
        tags=tags,
    )


def _cleanup_persistent(signum, frame):  # type: ignore
    """
    Clean up GitDB persistent

    Parameters
    ----------
    signum : int
        Signal number
    frame : frame
        Frame object
    """
    _ = signum, frame
    if PERSISTENT is not None and isinstance(PERSISTENT, GitDB):
        PERSISTENT.cleanup()


def _sigint_handler(signum, frame):  # type: ignore
    """
    Clean up GitDB persistent and raise KeyboardInterrupt as the default SIGINT handler

    Parameters
    ----------
    signum : int
        Signal number
    frame : frame
        Frame object

    Raises
    ------
    KeyboardInterrupt
        After performing persistent clean up
    """
    _cleanup_persistent(signum, frame)
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, _cleanup_persistent)
signal.signal(signal.SIGINT, _sigint_handler)

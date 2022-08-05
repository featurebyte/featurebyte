"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Callable

import signal

from bson.objectid import ObjectId
from fastapi import Depends, FastAPI, Request

import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_store.api as feature_store_api
from featurebyte.config import Configurations
from featurebyte.enum import CollectionName
from featurebyte.models.credential import Credential
from featurebyte.persistent import GitDB, Persistent
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_data.controller import EventDataController
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_store.controller import FeatureStoreController

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

        # GitDB configuration
        git_db.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])  # type: ignore

        PERSISTENT = git_db
    return PERSISTENT


def _get_credential(user_id: ObjectId | None, feature_store_name: str) -> Credential | None:
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
resource_api_controller_pairs = [
    (event_data_api, EventDataController),
    (entity_api, EntityController),
    (feature_api, FeatureController),
    (feature_store_api, FeatureStoreController),
]
for resource_api, resource_controller in resource_api_controller_pairs:
    app.include_router(
        resource_api.router,
        dependencies=[Depends(_get_api_deps(resource_controller))],
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

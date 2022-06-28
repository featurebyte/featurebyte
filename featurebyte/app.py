"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Callable

from bson.objectid import ObjectId
from fastapi import Depends, FastAPI, Request

import featurebyte.routes.event_data.api as event_data_api
from featurebyte.config import Configurations
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import DatabaseSourceModel
from featurebyte.persistent import GitDB, Persistent
from featurebyte.routes.event_data.controller import EventDataController

app = FastAPI()
PERSISTENT = None


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    # DO NOT CHANGE THIS VALUE
    id = ObjectId("62a6d9d023e7a8f2a0dc041a")


def get_persistent() -> Persistent:
    """
    Return global Persistent object
    Returns
    -------
    Persistent
        Persistent object
    """
    global PERSISTENT  # pylint: disable=global-statement
    if not PERSISTENT:
        config = Configurations()
        if not config.git:
            raise ValueError("Git settings not available in configurations")
        PERSISTENT = GitDB(**config.git.dict())
    return PERSISTENT


def get_credential(user_id: ObjectId, db_source: DatabaseSourceModel) -> Credential | None:
    """
    Retrieve credential from DatabaseSourceModel

    Parameters
    ----------
    user_id: ObjectId
        User ID
    db_source: DatabaseSourceModel
        DatabaseSourceModel object

    Returns
    -------
    Credential
        Credential for the database source
    """
    _ = user_id
    config = Configurations()
    return config.credentials.get(db_source)


def get_api_deps(controller: type) -> Callable[[Request], None]:
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
        request.state.persistent = get_persistent()
        request.state.user = User()
        request.state.get_credential = get_credential
        request.state.controller = controller

    return _dep_injection_func


app.include_router(event_data_api.router, dependencies=[Depends(get_api_deps(EventDataController))])

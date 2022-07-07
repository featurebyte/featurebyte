"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Callable

from bson.objectid import ObjectId
from fastapi import Depends, FastAPI, Request

import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
from featurebyte.config import Configurations
from featurebyte.enum import CollectionName
from featurebyte.models.credential import Credential
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.persistent import GitDB, Persistent
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_data.controller import EventDataController

app = FastAPI()
PERSISTENT = None


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
        git_db.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])

        PERSISTENT = git_db
    return PERSISTENT


def _get_credential(user_id: ObjectId, db_source: FeatureStoreModel) -> Credential | None:
    """
    Retrieve credential from FeatureStoreModel

    Parameters
    ----------
    user_id: ObjectId
        User ID
    db_source: FeatureStoreModel
        FeatureStoreModel object

    Returns
    -------
    Credential
        Credential for the database source
    """
    _ = user_id
    config = Configurations()
    return config.credentials.get(db_source)


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

    class User:
        """
        Skeleton user class to provide static user for API routes
        """

        id = None

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


app.include_router(
    event_data_api.router, dependencies=[Depends(_get_api_deps(EventDataController))]
)
app.include_router(entity_api.router, dependencies=[Depends(_get_api_deps(EntityController))])

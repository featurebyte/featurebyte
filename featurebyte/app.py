"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Callable

import signal

from fastapi import Depends, FastAPI, Request

import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_job_setting_analysis.api as feature_job_setting_analysis_api
import featurebyte.routes.feature_list.api as feature_list_api
import featurebyte.routes.feature_list_namespace.api as feature_list_namespace_api
import featurebyte.routes.feature_namespace.api as feature_namespace_api
import featurebyte.routes.feature_store.api as feature_store_api
import featurebyte.routes.task.api as task_api
import featurebyte.routes.temp_data.api as temp_data_api
from featurebyte.middleware import request_handler
from featurebyte.routes.app_container import AppContainer
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import cleanup_persistent, get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    id = None


def _get_api_deps() -> Callable[[Request], None]:
    """
    Get API dependency injection function

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
        request.state.get_storage = get_storage
        request.state.get_temp_storage = get_temp_storage

        request.state.app_container = AppContainer.get_instance(
            user=request.state.user,
            persistent=get_persistent(),
            temp_storage=get_temp_storage(),
            task_manager=TaskManager(user_id=request.state.user.id),
            storage=get_storage(),
        )

    return _dep_injection_func


def get_app() -> FastAPI:
    """
    Get FastAPI object

    Returns
    -------
    FastAPI
        FastAPI object
    """
    app = FastAPI()

    # add routers into the app
    resource_apis = [
        event_data_api,
        entity_api,
        feature_api,
        feature_job_setting_analysis_api,
        feature_list_api,
        feature_list_namespace_api,
        feature_namespace_api,
        feature_store_api,
        task_api,
        temp_data_api,
    ]
    for resource_api in resource_apis:
        app.include_router(
            resource_api.router,
            dependencies=[Depends(_get_api_deps())],
            tags=[resource_api.router.prefix[1:]],
        )

    app.middleware("http")(request_handler)
    return app


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
    cleanup_persistent(signum, frame)
    raise KeyboardInterrupt


app = get_app()

signal.signal(signal.SIGTERM, cleanup_persistent)
signal.signal(signal.SIGINT, _sigint_handler)

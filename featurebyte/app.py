"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Callable

import signal

from fastapi import Depends, FastAPI, Request

import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_job_setting_analysis.api as feature_job_setting_analysis_api
import featurebyte.routes.feature_list.api as feature_list_api
import featurebyte.routes.feature_namespace.api as feature_namespace_api
import featurebyte.routes.feature_store.api as feature_store_api
import featurebyte.routes.task.api as task_api
from featurebyte.routes.common.base import BaseController
from featurebyte.routes.entity.controller import EntityController
from featurebyte.routes.event_data.controller import EventDataController
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_job_setting_analysis.controller import (
    FeatureJobSettingAnalysisController,
)
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.routes.feature_namespace.controller import FeatureNamespaceController
from featurebyte.routes.feature_store.controller import FeatureStoreController
from featurebyte.routes.task.controller import TaskController
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import cleanup_persistent, get_persistent

app = FastAPI()


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    id = None


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
        request.state.persistent = get_persistent()
        request.state.user = User()
        request.state.task_manager = TaskManager(user_id=request.state.user.id)
        request.state.get_credential = get_credential
        request.state.controller = controller

    return _dep_injection_func


# add routers into the app
resource_api_controller_pairs: list[tuple[Any, type[BaseController[Any, Any]]]] = [
    (event_data_api, EventDataController),
    (entity_api, EntityController),
    (feature_api, FeatureController),
    (feature_job_setting_analysis_api, FeatureJobSettingAnalysisController),
    (feature_list_api, FeatureListController),
    (feature_namespace_api, FeatureNamespaceController),
    (feature_store_api, FeatureStoreController),
]
for resource_api, resource_controller in resource_api_controller_pairs:
    app.include_router(
        resource_api.router,
        dependencies=[Depends(_get_api_deps(resource_controller))],
        tags=[resource_controller.collection_name],
    )

# add non-persistent-storage route
app.include_router(task_api.router, dependencies=[Depends(_get_api_deps(TaskController))])


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


signal.signal(signal.SIGTERM, cleanup_persistent)
signal.signal(signal.SIGINT, _sigint_handler)

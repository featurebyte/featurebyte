"""
FastAPI Application
"""
from typing import Callable, Optional

import uvicorn
from bson import ObjectId
from fastapi import Depends, FastAPI, Request
from pydantic import Field

import featurebyte.routes.context.api as context_api
import featurebyte.routes.dimension_data.api as dimension_data_api
import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_data.api as event_data_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_job_setting_analysis.api as feature_job_setting_analysis_api
import featurebyte.routes.feature_list.api as feature_list_api
import featurebyte.routes.feature_list_namespace.api as feature_list_namespace_api
import featurebyte.routes.feature_namespace.api as feature_namespace_api
import featurebyte.routes.feature_store.api as feature_store_api
import featurebyte.routes.item_data.api as item_data_api
import featurebyte.routes.scd_data.api as scd_data_api
import featurebyte.routes.semantic.api as semantic_api
import featurebyte.routes.tabular_data.api as tabular_data_api
import featurebyte.routes.task.api as task_api
import featurebyte.routes.temp_data.api as temp_data_api
import featurebyte.routes.workspace.api as workspace_api
from featurebyte.common.utils import get_version
from featurebyte.middleware import request_handler
from featurebyte.models.base import DEFAULT_WORKSPACE_ID, FeatureByteBaseModel, PydanticObjectId
from featurebyte.routes.app_container import AppContainer
from featurebyte.schema import APIServiceStatus
from featurebyte.service.task_manager import TaskManager
from featurebyte.utils.credential import ConfigCredentialProvider
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage


class User(FeatureByteBaseModel):
    """
    Skeleton user class to provide static user for API routes
    """

    id: Optional[PydanticObjectId] = Field(default=None)


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
        request.state.get_credential = ConfigCredentialProvider().get_credential
        request.state.get_storage = get_storage
        request.state.get_temp_storage = get_temp_storage

        request.state.app_container = AppContainer.get_instance(
            user=request.state.user,
            persistent=get_persistent(),
            temp_storage=get_temp_storage(),
            task_manager=TaskManager(user_id=request.state.user.id),
            storage=get_storage(),
            workspace_id=ObjectId(request.query_params.get("workspace_id", DEFAULT_WORKSPACE_ID)),
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
    _app = FastAPI()

    # add routers into the app
    resource_apis = [
        context_api,
        dimension_data_api,
        event_data_api,
        item_data_api,
        entity_api,
        feature_api,
        feature_job_setting_analysis_api,
        feature_list_api,
        feature_list_namespace_api,
        feature_namespace_api,
        feature_store_api,
        scd_data_api,
        semantic_api,
        tabular_data_api,
        task_api,
        temp_data_api,
        workspace_api,
    ]
    dependencies = _get_api_deps()
    for resource_api in resource_apis:
        _app.include_router(
            resource_api.router,
            dependencies=[Depends(dependencies)],
            tags=[resource_api.router.prefix[1:]],
        )

    @_app.get("/status", description="Get API status.", response_model=APIServiceStatus)
    async def get_status() -> APIServiceStatus:
        """
        Service alive health check

        Returns
        -------
        APIServiceStatus
            APIServiceStatus object
        """
        return APIServiceStatus(sdk_version=get_version())

    _app.middleware("http")(request_handler)
    return _app


app = get_app()


if __name__ == "__main__":
    # for debugging the api service
    uvicorn.run(app, host="127.0.0.1", port=8000)

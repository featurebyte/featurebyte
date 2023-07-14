"""
FastAPI Application
"""
from typing import Any, Callable, Coroutine, Optional

import aioredis
import uvicorn
from fastapi import Depends, FastAPI, Header, Request
from starlette.websockets import WebSocket

import featurebyte.routes.batch_feature_table.api as batch_feature_table_api
import featurebyte.routes.batch_request_table.api as batch_request_table_api
import featurebyte.routes.catalog.api as catalog_api
import featurebyte.routes.context.api as context_api
import featurebyte.routes.credential.api as credential_api
import featurebyte.routes.deployment.api as deployment_api
import featurebyte.routes.dimension_table.api as dimension_table_api
import featurebyte.routes.entity.api as entity_api
import featurebyte.routes.event_table.api as event_table_api
import featurebyte.routes.feature.api as feature_api
import featurebyte.routes.feature_job_setting_analysis.api as feature_job_setting_analysis_api
import featurebyte.routes.feature_list.api as feature_list_api
import featurebyte.routes.feature_list_namespace.api as feature_list_namespace_api
import featurebyte.routes.feature_namespace.api as feature_namespace_api
import featurebyte.routes.feature_store.api as feature_store_api
import featurebyte.routes.historical_feature_table.api as historical_feature_table_api
import featurebyte.routes.item_table.api as item_table_api
import featurebyte.routes.observation_table.api as observation_table_api
import featurebyte.routes.periodic_tasks.api as periodic_tasks_api
import featurebyte.routes.relationship_info.api as relationship_info_api
import featurebyte.routes.scd_table.api as scd_table_api
import featurebyte.routes.semantic.api as semantic_api
import featurebyte.routes.static_source_table.api as static_source_table_api
import featurebyte.routes.table.api as table_api
import featurebyte.routes.target.api as target_api
import featurebyte.routes.target_namespace.api as target_namespace_api
import featurebyte.routes.target_table.api as target_table_api
import featurebyte.routes.task.api as task_api
import featurebyte.routes.temp_data.api as temp_data_api
import featurebyte.routes.user_defined_function.api as user_defined_function_api
from featurebyte.common.utils import get_version
from featurebyte.logging import get_logger
from featurebyte.middleware import ExceptionMiddleware
from featurebyte.models.base import PydanticObjectId, User
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config
from featurebyte.schema import APIServiceStatus
from featurebyte.schema.task import TaskId
from featurebyte.utils.credential import MongoBackedCredentialProvider
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import get_persistent
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import get_celery

logger = get_logger(__name__)


def _dep_injection_func(
    request: Request, active_catalog_id: Optional[PydanticObjectId] = None
) -> None:
    """
    Inject dependencies into the requests

    Parameters
    ----------
    request: Request
        Request object to be updated
    active_catalog_id: Optional[PydanticObjectId]
        Catalog ID to be used for the request
    """
    request.state.persistent = get_persistent()
    request.state.user = User()
    request.state.get_credential = MongoBackedCredentialProvider(
        persistent=request.state.persistent
    ).get_credential
    request.state.app_container = LazyAppContainer(
        user=request.state.user,
        persistent=request.state.persistent,
        temp_storage=get_temp_storage(),
        celery=get_celery(),
        storage=get_storage(),
        catalog_id=active_catalog_id,
        app_container_config=app_container_config,
    )


def _get_api_deps() -> Callable[[Request], Coroutine[Any, Any, None]]:
    """
    Get API dependency injection function

    Returns
    -------
    Callable[Request]
        Dependency injection function
    """

    async def _wrapper(
        request: Request,
    ) -> None:
        _dep_injection_func(request)

    return _wrapper


def _get_api_deps_with_catalog() -> Callable[[Request], Coroutine[Any, Any, None]]:
    """
    Get API dependency injection function with catalog

    Returns
    -------
    Callable[Request]
        Dependency injection function
    """

    async def _wrapper(
        request: Request,
        active_catalog_id: Optional[PydanticObjectId] = Header(None),
    ) -> None:
        _dep_injection_func(request, active_catalog_id)

    return _wrapper


def get_app() -> FastAPI:
    """
    Get FastAPI object

    Returns
    -------
    FastAPI
        FastAPI object
    """
    _app = FastAPI()

    # register routes that are not catalog-specific
    resource_apis = [
        credential_api,
        feature_store_api,
        semantic_api,
        task_api,
        temp_data_api,
        catalog_api,
    ]
    dependencies = _get_api_deps()
    for resource_api in resource_apis:
        _app.include_router(
            resource_api.router,
            dependencies=[Depends(dependencies)],
            tags=[resource_api.router.prefix[1:]],
        )

    # register routes that are catalog-specific
    resource_apis = [
        context_api,
        deployment_api,
        dimension_table_api,
        event_table_api,
        item_table_api,
        entity_api,
        feature_api,
        feature_job_setting_analysis_api,
        feature_list_api,
        feature_list_namespace_api,
        feature_namespace_api,
        relationship_info_api,
        scd_table_api,
        static_source_table_api,
        table_api,
        observation_table_api,
        historical_feature_table_api,
        batch_request_table_api,
        batch_feature_table_api,
        target_api,
        target_namespace_api,
        periodic_tasks_api,
        user_defined_function_api,
        target_table_api,
    ]
    dependencies = _get_api_deps_with_catalog()
    for resource_api in resource_apis:
        _app.include_router(
            resource_api.router,
            dependencies=[Depends(dependencies)],
            tags=[resource_api.router.prefix[1:]],
        )

    @_app.get("/status", description="Get API status.", response_model=APIServiceStatus)
    async def get_status() -> APIServiceStatus:
        """
        Service alive health check.

        Returns
        -------
        APIServiceStatus
            APIServiceStatus object.
        """
        return APIServiceStatus(sdk_version=get_version())

    @_app.websocket("/ws/{task_id}")
    async def websocket_endpoint(
        websocket: WebSocket,
        task_id: TaskId,
    ) -> None:
        """
        Websocket for getting task progress updates.

        Parameters
        ----------
        websocket: WebSocket
            Websocket object.
        task_id: TaskId
            Task ID.
        """
        await websocket.accept()
        user = User()
        channel = f"task_{user.id}_{task_id}_progress"

        logger.debug("Listening to channel", extra={"channel": channel})
        redis = await aioredis.from_url(REDIS_URI)
        sub = redis.pubsub()
        await sub.subscribe(channel)

        # listen for messages
        async for message in sub.listen():
            if message and isinstance(message, dict):
                data = message.get("data")
                if isinstance(data, bytes):
                    await websocket.send_bytes(data)

        # clean up
        logger.debug("Unsubscribing from channel", extra={"channel": channel})
        await sub.unsubscribe(channel)
        await sub.close()
        redis.close()

    # Add exception middleware
    _app.add_middleware(ExceptionMiddleware)

    return _app


app = get_app()


if __name__ == "__main__":
    # for debugging the api service
    uvicorn.run(app, host="127.0.0.1", port=8000)

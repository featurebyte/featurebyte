"""
FastAPI Application
"""

from typing import Any, Callable, Coroutine, List, Optional

import redis.asyncio as redis
import uvicorn
from fastapi import Depends, FastAPI, Header, Request
from starlette.websockets import WebSocket

from featurebyte._overrides.typechecked_override import custom_typechecked
from featurebyte.common.utils import get_version
from featurebyte.logging import configure_featurebyte_logger, get_logger
from featurebyte.middleware import ExceptionMiddleware
from featurebyte.models.base import PydanticObjectId, User
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.batch_feature_table.api import BatchFeatureTableRouter
from featurebyte.routes.batch_request_table.api import BatchRequestTableRouter
from featurebyte.routes.catalog.api import CatalogRouter
from featurebyte.routes.context.api import ContextRouter
from featurebyte.routes.credential.api import CredentialRouter
from featurebyte.routes.deployment.api import DeploymentRouter
from featurebyte.routes.dimension_table.api import DimensionTableRouter
from featurebyte.routes.entity.api import EntityRouter
from featurebyte.routes.event_table.api import EventTableRouter
from featurebyte.routes.feature.api import FeatureRouter
from featurebyte.routes.feature_job_setting_analysis.api import FeatureJobSettingAnalysisRouter
from featurebyte.routes.feature_list.api import FeatureListRouter
from featurebyte.routes.feature_list_namespace.api import FeatureListNamespaceRouter
from featurebyte.routes.feature_namespace.api import FeatureNamespaceRouter
from featurebyte.routes.feature_store.api import FeatureStoreRouter
from featurebyte.routes.historical_feature_table.api import HistoricalFeatureTableRouter
from featurebyte.routes.item_table.api import ItemTableRouter
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.observation_table.api import ObservationTableRouter
from featurebyte.routes.online_store.api import OnlineStoreRouter
from featurebyte.routes.periodic_tasks.api import PeriodicTaskRouter
from featurebyte.routes.registry import app_container_config
from featurebyte.routes.relationship_info.api import RelationshipInfoRouter
from featurebyte.routes.scd_table.api import SCDTableRouter
from featurebyte.routes.semantic.api import SemanticRouter
from featurebyte.routes.static_source_table.api import StaticSourceTableRouter
from featurebyte.routes.table.api import TableRouter
from featurebyte.routes.target.api import TargetRouter
from featurebyte.routes.target_namespace.api import TargetNamespaceRouter
from featurebyte.routes.target_table.api import TargetTableRouter
from featurebyte.routes.task.api import TaskRouter
from featurebyte.routes.temp_data.api import TempDataRouter
from featurebyte.routes.use_case.api import UseCaseRouter
from featurebyte.routes.user_defined_function.api import UserDefinedFunctionRouter
from featurebyte.schema import APIServiceStatus
from featurebyte.schema.task import TaskId
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import MongoDBImpl
from featurebyte.utils.storage import get_storage, get_temp_storage
from featurebyte.worker import get_celery, get_redis

configure_featurebyte_logger()
logger = get_logger(__name__)

# import to override typechecked decorator
_ = custom_typechecked


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
    request.state.user = User()
    request.state.app_container = LazyAppContainer(
        user=request.state.user,
        persistent=MongoDBImpl(),
        storage=get_storage(),
        temp_storage=get_temp_storage(),
        redis=get_redis(),
        celery=get_celery(),
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

    # Register routers that are not catalog-specific
    non_catalog_specific_routers: List[BaseRouter] = [
        CatalogRouter(),
        CredentialRouter(),
        FeatureStoreRouter(),
        SemanticRouter(),
        TaskRouter(),
        TempDataRouter(),
        OnlineStoreRouter(),
    ]
    dependencies = _get_api_deps()
    for resource_api in non_catalog_specific_routers:
        _app.include_router(
            resource_api.router,
            dependencies=[Depends(dependencies)],
            tags=[resource_api.router.prefix[1:]],
        )

    # Register routes that are catalog-specific
    catalog_specific_routers: List[BaseRouter] = [
        BatchFeatureTableRouter(),
        BatchRequestTableRouter(),
        ContextRouter(),
        DeploymentRouter(),
        DimensionTableRouter(),
        EntityRouter(),
        EventTableRouter(),
        FeatureRouter(),
        FeatureJobSettingAnalysisRouter(),
        FeatureListRouter(),
        FeatureListNamespaceRouter(),
        FeatureNamespaceRouter(),
        HistoricalFeatureTableRouter(),
        ItemTableRouter(),
        ObservationTableRouter(),
        PeriodicTaskRouter(),
        RelationshipInfoRouter(),
        SCDTableRouter(),
        StaticSourceTableRouter(prefix="/static_source_table"),
        TableRouter(),
        TargetRouter(),
        TargetNamespaceRouter(),
        TargetTableRouter(prefix="/target_table"),
        UseCaseRouter(),
        UserDefinedFunctionRouter(),
    ]
    dependencies = _get_api_deps_with_catalog()
    for resource_api in catalog_specific_routers:
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

        async with redis.from_url(REDIS_URI) as client:
            async with client.pubsub() as pubsub:  # type: ignore
                logger.debug("Listening to channel", extra={"channel": channel})
                await pubsub.subscribe(channel)
                async for message in pubsub.listen():
                    if message and isinstance(message, dict):
                        data = message.get("data")
                        if isinstance(data, bytes):
                            await websocket.send_bytes(data)

    # Add exception middleware
    _app.add_middleware(ExceptionMiddleware)

    return _app


app = get_app()


if __name__ == "__main__":
    # for debugging the api service
    uvicorn.run(app, host="127.0.0.1", port=8000)

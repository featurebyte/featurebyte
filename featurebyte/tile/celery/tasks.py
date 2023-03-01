"""
Celery Task Definition and Wrapper
"""
import asyncio
import importlib
import json

from celery import Celery

from featurebyte.models.base import PydanticObjectId, User
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.manager import SessionManager
from featurebyte.tile.celery.server import redis_server
from featurebyte.utils.credential import get_credential
from featurebyte.utils.persistent import get_persistent

celery_app = Celery(
    "featurebyte_tasks", backend=redis_server.redis_url, broker=redis_server.redis_url
)
celery_app.config_from_object(
    {
        "redbeat_redis_url": f"{redis_server.redis_url}/1",
        "beat_max_loop_interval": 5,
        "redbeat_key_prefix": "redbeat-",
        "redbeat_lock_key": None,
    }
)


async def execute_internal(module_path: str, class_name: str, instance_str: str) -> None:
    """
    Async execute for Tile Schedule classes

    Parameters
    ----------
    module_path: str
        module path
    class_name: str
        class name
    instance_str: str
        pydantic json string for the class
    """

    module = importlib.import_module(module_path)
    instance_class = getattr(module, class_name)
    instance_json = json.loads(instance_str)

    user_id = instance_json["user_id"]
    feature_store_id = instance_json["feature_store_id"]
    workspace_id = instance_json["workspace_id"]

    user = User(id=PydanticObjectId(user_id))
    feature_store_service = FeatureStoreService(
        user=user, persistent=get_persistent(), workspace_id=workspace_id
    )
    feature_store = await feature_store_service.get_document(document_id=feature_store_id)

    # establish database session
    session_manager = SessionManager(
        credentials={
            feature_store.name: await get_credential(
                user_id=user_id, feature_store_name=feature_store.name
            )
        }
    )
    db_session = await session_manager.get_session(feature_store)

    instance = instance_class(spark_session=db_session, **instance_json)

    await instance.execute()


@celery_app.task
def execute(module_path: str, class_name: str, instance_str: str) -> None:
    """
    Formal Celery Task definition

    Parameters
    ----------
    module_path: str
        module path
    class_name: str
        class name
    instance_str: str
        pydantic json string for the class
    """
    asyncio.run(execute_internal(module_path, class_name, instance_str))

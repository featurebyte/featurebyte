"""
Celery Task Definition and Wrapper
"""
import asyncio
import importlib
import json

from celery import Celery

from featurebyte.tile.celery.server import redis_server

celery_app = Celery(
    "featurebyte_tasks", backend=redis_server.redis_url, broker=redis_server.redis_url
)
celery_app.config_from_object(
    {
        "redbeat_redis_url": f"{redis_server.redis_url}/1",
        "beat_max_loop_interval": 5,
        "redbeat_key_prefix": "redbeat",
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
    instance = instance_class(**instance_json)

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

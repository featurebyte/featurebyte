"""
Celery worker
"""
from typing import Any

from asyncio import get_event_loop
from inspect import isawaitable

from celery import Celery

from featurebyte.models.task import Task
from featurebyte.utils.messaging import REDIS_URI
from featurebyte.utils.persistent import DATABASE_NAME, MONGO_URI

CELERY_TASK_COLLECTION = Task.collection_name()
CELERY_SCHEDULE_COLLECTION = "schedules"


class AsyncCelery(Celery):
    """
    Customized Celery to support asynchronous tasks
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.patch_task()

        if "app" in kwargs:
            self.init_app(kwargs["app"])

    def patch_task(self) -> None:
        """
        Patch Task to support asynchronous tasks using process event loop
        """
        TaskBase = self.Task

        class ContextTask(TaskBase):  # type: ignore
            """
            Async Task class
            """

            abstract = True

            async def _run(self, *args: Any, **kwargs: Any) -> Any:
                result = TaskBase.__call__(self, *args, **kwargs)
                if isawaitable(result):
                    return await result
                return result

            def __call__(self, *args: Any, **kwargs: Any) -> Any:
                loop = get_event_loop()
                return loop.run_until_complete(self._run(*args, **kwargs))

        self.Task = ContextTask  # pylint: disable=invalid-name


celery = AsyncCelery(
    __name__,
    broker=REDIS_URI,
    backend=MONGO_URI,
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    enable_utc=True,
)

# Celery configuration options:
# https://docs.celeryq.dev/en/stable/userguide/configuration.html

# celery behavior
celery.conf.task_track_started = True

# task queues and routing
celery.conf.task_routes = {"worker.*": "celery"}

# task tombstones options
celery.conf.result_extended = True
celery.conf.result_expires = 15  # 15 days expiry
celery.conf.result_backend_always_retry = False
celery.conf.mongodb_backend_settings = {
    "database": DATABASE_NAME,
    "taskmeta_collection": CELERY_TASK_COLLECTION,
}

# beat scheduler
celery.conf.mongodb_scheduler_collection = CELERY_SCHEDULE_COLLECTION
celery.conf.mongodb_scheduler_url = MONGO_URI

"""
Start worker
"""

from featurebyte.worker import get_celery, initialize_asyncio_event_loop
from featurebyte.worker.task_executor import CPUBoundTask, IOBoundTask

# TODO: Remove this worker
celery = get_celery()
celery.register_task(CPUBoundTask())
celery.register_task(IOBoundTask())
initialize_asyncio_event_loop()

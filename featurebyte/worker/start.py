"""
Start worker
"""
from . import celery

celery.autodiscover_tasks(["featurebyte.worker.task_executor"])

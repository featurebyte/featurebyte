"""
Start worker
"""
from .task_executor import celery

celery.autodiscover_tasks(["featurebyte.worker.task_executor"])

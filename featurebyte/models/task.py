"""
Task document model
"""
from typing import Any, Dict, List, Optional

from datetime import datetime
from uuid import UUID, uuid4

import pymongo
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseDocumentModel


class Task(FeatureByteBaseDocumentModel):
    """
    Task document model
    """

    id: UUID = Field(default_factory=uuid4, alias="_id")  # type: ignore
    status: str
    result: str
    traceback: Optional[str]
    children: List[str]
    date_done: datetime
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    worker: str
    retries: int
    queue: str

    class Settings:
        """
        Collection settings for celery taskmeta document
        """

        collection_name = "celery_taskmeta"
        indexes = [
            pymongo.operations.IndexModel("date_done"),
            [
                ("name", pymongo.TEXT),
                ("worker", pymongo.TEXT),
                ("queue", pymongo.TEXT),
                ("args", pymongo.TEXT),
                ("kwargs", pymongo.TEXT),
            ],
        ]

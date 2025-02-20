"""
Task document model
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

import pymongo
from pydantic import BaseModel, Field

from featurebyte.models.base import FeatureByteBaseDocumentModel


class LogMessage(BaseModel):
    """Log message schema"""

    percent: int
    message: Optional[str] = None
    timestamp: Optional[datetime] = Field(default=None)


class ProgressHistory(BaseModel):
    """
    Progress history schema
    """

    data: List[LogMessage] = Field(default=[])
    # compress_at is the last percent at which compression was done
    # if compress_at is 0, it means no compression has been done
    # if compress_at is 100, it means all messages have been compressed
    compress_at: int = Field(default=0)

    def compress(self, max_messages: int) -> "ProgressHistory":
        """
        Compress progress history

        Parameters
        ----------
        max_messages : int
            Maximum number of messages to keep

        Returns
        -------
        ProgressHistory
            Compressed progress history
        """
        if len(self.data) <= max_messages:
            return self

        # Step 1: Map percents to message indices
        percent_to_indices = defaultdict(list)
        for idx, msg in enumerate(self.data):
            percent_to_indices[msg.percent].append(idx)

        # Step 2: Initialize variables
        compressed_indices = set()
        total_messages = len(self.data)
        current_percent = self.compress_at

        # Step 3: Iterate over percents from compress_at upwards
        for percent in range(self.compress_at, 101):
            indices = percent_to_indices.get(percent, [])
            if len(indices) > 1:
                # Keep only the last message, remove the rest
                indices_to_remove = indices[:-1]
                compressed_indices.update(indices_to_remove)
                total_messages -= len(indices_to_remove)

            # Update compress_at to the current percent
            current_percent = percent

            # Stop if total messages are within the limit
            if total_messages <= max_messages:
                break

            # Stop if percent reaches maximum
            if percent == self.data[-1].percent:
                break

        # Step 4: Update data and compress_at
        messages = [msg for idx, msg in enumerate(self.data) if idx not in compressed_indices]
        self.data = messages[-max_messages:]
        self.compress_at = current_percent

        return self


class Task(FeatureByteBaseDocumentModel):
    """
    Task document model
    """

    id: UUID = Field(default_factory=uuid4, alias="_id")  # type: ignore
    status: str
    result: Optional[str] = Field(default=None)
    traceback: Optional[str] = Field(default=None)
    children: List[str]
    start_time: Optional[datetime] = Field(default=None)
    date_done: Optional[datetime] = Field(default=None)
    name: str
    args: List[Any]
    kwargs: Dict[str, Any]
    worker: Optional[str] = Field(default=None)
    retries: int
    queue: str
    progress: Optional[Dict[str, Any]] = Field(default=None)
    progress_history: Optional[ProgressHistory] = Field(default=None)

    class Settings:
        """
        Collection settings for celery taskmeta document
        """

        collection_name = "celery_taskmeta"
        indexes = [
            pymongo.operations.IndexModel("name"),
            pymongo.operations.IndexModel("start_time", expireAfterSeconds=3600 * 24 * 30),
            pymongo.operations.IndexModel("kwargs.is_scheduled_task"),
            pymongo.operations.IndexModel("kwargs.catalog_id"),
            pymongo.operations.IndexModel("date_done"),
            [
                ("name", pymongo.TEXT),
                ("worker", pymongo.TEXT),
                ("queue", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
        auditable = False

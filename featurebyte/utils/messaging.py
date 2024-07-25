"""
Support for asynchronous messaging via Redis and web sockets
"""

import json
import os
from typing import Any, Dict, Optional, cast
from uuid import UUID

import redis
from bson import ObjectId

REDIS_URI = os.environ.get("REDIS_URI", "redis://localhost:6379")


class Progress:
    """
    Progress object
    """

    def __init__(self, user_id: Optional[ObjectId], task_id: UUID, redis_uri: str = REDIS_URI):
        """
        Initialize progress object

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID
        task_id: UUID
            Task ID
        redis_uri: str
            Redis URI
        """
        self._redis = redis.from_url(redis_uri)
        self._channel = f"task_{user_id}_{task_id}_progress"

    def put(self, message: Dict[str, Any]) -> None:
        """
        Publish to channel

        Parameters
        ----------
        message: Dict[str, Any]
            Message to publish
        """
        self._redis.publish(self._channel, json.dumps(message))

    def get(self) -> Optional[Dict[str, Any]]:
        """
        Get message from channel

        Returns
        -------
        Optional[Dict[str, Any]]
        """
        message = self._redis.get(self._channel)
        if message is None:
            return None
        return cast(Dict[str, Any], json.loads(message))

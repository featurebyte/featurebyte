"""
Support for asynchronous messaging via Redis and web sockets
"""

from typing import Any, Dict, Optional, cast

import json
import os
from uuid import UUID

import redis
from bson.objectid import ObjectId

from featurebyte.logging import get_logger

REDIS_URI = os.environ.get("REDIS_URI", "redis://localhost:6379")


logger = get_logger(__name__)


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
        logger.debug("Publishing to channel", extra={"channel": self._channel})

    def put(self, message: Dict[str, Any]) -> None:
        """
        Publish to channel

        Parameters
        ----------
        message: Dict[str, Any]
            Message to publish
        """
        logger.debug("Publishing to channel", extra={"message": message})
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

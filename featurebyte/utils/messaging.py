"""
Support for asynchronous messaging via Redis and web sockets
"""

import json
import os
from typing import Any, Dict, Optional, cast
from uuid import UUID

from bson import ObjectId
from redis.asyncio import Redis

REDIS_URI = os.environ.get("REDIS_URI", "redis://localhost:6379")


class Progress:
    """
    Progress object
    """

    def __init__(self, user_id: Optional[ObjectId], task_id: UUID):
        """
        Initialize progress object

        Parameters
        ----------
        user_id: Optional[ObjectId]
            User ID
        task_id: UUID
            Task ID
        """
        self._redis = self._init_redis()
        self._channel = f"task_{user_id}_{task_id}_progress"

    @staticmethod
    def _init_redis() -> Any:
        """
        Initialize Redis connection

        Returns
        -------
        Any
            Redis client
        """
        return Redis.from_url(REDIS_URI)

    async def put(self, message: Dict[str, Any]) -> None:
        """
        Publish to channel

        Parameters
        ----------
        message: Dict[str, Any]
            Message to publish
        """
        await self._redis.publish(self._channel, json.dumps(message))

    async def get(self) -> Optional[Dict[str, Any]]:
        """
        Get message from channel

        Returns
        -------
        Optional[Dict[str, Any]]
        """
        message = await self._redis.get(self._channel)
        if message is None:
            return None
        return cast(Dict[str, Any], json.loads(message))

    async def close(self) -> None:
        """
        Close progress object
        """
        await self._redis.close()

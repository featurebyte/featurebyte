"""
SessionManager class
"""
from __future__ import annotations

from typing import Dict, Optional

import json

from cachetools import TTLCache, cached
from pydantic import BaseModel

from featurebyte.enum import SourceType
from featurebyte.logger import logger
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import DatabaseSourceModel
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.session.sqlite import SQLiteSession

SOURCE_TYPE_SESSION_MAP = {
    SourceType.SQLITE: SQLiteSession,
    SourceType.SNOWFLAKE: SnowflakeSession,
}


class SessionManager(BaseModel):
    """
    Session manager to manage session of different database sources
    """

    credentials: Dict[DatabaseSourceModel, Optional[Credential]]

    def __hash__(self) -> int:
        return hash(
            json.dumps(
                {
                    hash(key): value.json() if isinstance(value, BaseModel) else value
                    for key, value in self.credentials.items()
                }
            )
        )

    @cached(cache=TTLCache(maxsize=1024, ttl=1800))
    def __getitem__(self, item: DatabaseSourceModel) -> BaseSession:
        logger.debug(f"Create a new session for {item.type.value}")
        credential = self.credentials[item]
        credential_params = credential.credential.dict() if credential else {}
        return SOURCE_TYPE_SESSION_MAP[item.type](**item.details.dict(), **credential_params)

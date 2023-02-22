"""
BaseService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.persistent.base import Persistent
from featurebyte.service.mixin import OpsServiceMixin


class BaseService(OpsServiceMixin):
    """
    BaseService class has access to all document services as property.
    """

    def __init__(self, user: Any, persistent: Persistent, workspace_id: ObjectId):
        self.user = user
        self.persistent = persistent
        self.workspace_id = workspace_id

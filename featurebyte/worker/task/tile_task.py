"""
Test task
"""
from __future__ import annotations

from typing import Any, cast

import importlib
import json

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.tile import TileTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.manager import SessionManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class TileTask(BaseTask):
    """
    Test Task
    """

    payload_class = TileTaskPayload

    async def execute(self) -> Any:
        """
        Execute Tile task
        """
        logger.debug("Tile task started")

        payload = cast(TileTaskPayload, self.payload)

        module = importlib.import_module(payload.module_path)
        instance_class = getattr(module, payload.class_name)
        instance_json = json.loads(payload.instance_str)

        logger.debug(f"module: {payload.module_path}")
        logger.debug(f"class_name: {payload.class_name}")
        logger.debug(f"instance_str: {payload.instance_str}")

        # get feature store
        feature_store_service = FeatureStoreService(
            user=self.user,
            persistent=self.get_persistent(),
            catalog_id=payload.catalog_id,
        )
        feature_store = await feature_store_service.get_document(
            document_id=payload.feature_store_id
        )

        # establish database session
        session_manager = SessionManager(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=payload.user_id, feature_store_name=feature_store.name
                )
            }
        )
        db_session = await session_manager.get_session(feature_store)

        instance = instance_class(session=db_session, **instance_json)

        await instance.execute()

        logger.debug("Tile task ended")

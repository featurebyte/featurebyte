"""
WorkingSchemaService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.persistent import Persistent
from featurebyte.service.base_service import BaseService
from featurebyte.service.feature import FeatureService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.session.base import BaseSession, MetadataSchemaInitializer


class WorkingSchemaService(BaseService):
    """
    WorkingSchemaService
    """

    def __init__(self, user: Any, persistent: Persistent):
        super().__init__(user, persistent)
        self.feature_service = FeatureService(user=user, persistent=persistent)

    async def recreate_working_schema(
        self, feature_store_id: ObjectId, session: BaseSession
    ) -> None:
        """
        Resets the data warehouse working schema by dropping everything and recreating

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        session: BaseSession
            BaseSession object
        """

        # Drop everything in the working schema. It would be easier to drop the schema directly and
        # then recreate, but the assumption is that the account might not have this privilege.
        initializer = session.initializer()
        if initializer is not None:
            try:
                await initializer.drop_all_objects_in_working_schema()
            except NotImplementedError:
                logger.info(f"drop_all_objects_in_working_schema not implemented for {session}")
                return
        else:
            return

        # Initialize working schema. This covers registering tables, functions and procedures.
        await initializer.initialize()

        # Update feature store id in the metadata schema. This is typically done on creation of
        # FeatureStore, so as the working schema is created from scratch it has to be done again.
        await MetadataSchemaInitializer(session).update_feature_store_id(str(feature_store_id))

        # Reschedule jobs. Only online enabled features need to be handled. For not yet online
        # enabled features, historical requests can still work as the tiles are calculated on
        # demand.
        await self._reschedule_online_enabled_features(feature_store_id, session)

    async def _reschedule_online_enabled_features(
        self, feature_store_id: ObjectId, session: BaseSession
    ) -> None:

        online_enabled_feature_docs = self.feature_service.list_documents_iterator(
            {"tabular_source.feature_store_id": feature_store_id, "online_enabled": True}
        )

        async for feature_doc in online_enabled_feature_docs:
            logger.info(f'Rescheduling jobs for online enabled feature: {feature_doc["name"]}')
            feature = FeatureModel(**feature_doc)
            await OnlineEnableService.update_data_warehouse_with_session(
                session=session, feature=feature
            )

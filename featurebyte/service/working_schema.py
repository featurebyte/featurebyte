"""
WorkingSchemaService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.persistent import Persistent
from featurebyte.service.deployed_tile_table import DeployedTileTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_manager import FeatureManagerService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.tile_registry_service import TileRegistryService
from featurebyte.session.base import BaseSession, MetadataSchemaInitializer

logger = get_logger(__name__)


async def drop_all_objects(session: BaseSession) -> None:
    """
    Drop all objects in the working schema

    Parameters
    ----------
    session: BaseSession
        BaseSession object
    """
    initializer = session.initializer()
    if initializer is not None:
        try:
            await initializer.drop_all_objects_in_working_schema()
        except NotImplementedError:
            logger.info(f"drop_all_objects_in_working_schema not implemented for {session}")
            return
    else:
        return


class WorkingSchemaService:
    """
    WorkingSchemaService is responsible for managing the working schema in the data warehouse
    """

    def __init__(
        self,
        user: User,
        persistent: Persistent,
        feature_service: FeatureService,
        feature_manager_service: FeatureManagerService,
        tile_registry_service: TileRegistryService,
        deployed_tile_table_service: DeployedTileTableService,
    ):
        self.user = user
        self.persistent = persistent
        self.feature_service = feature_service
        self.feature_manager_service = feature_manager_service
        self.tile_registry_service = tile_registry_service
        self.deployed_tile_table_service = deployed_tile_table_service

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
        await drop_all_objects(session)

        # Clear the tile registry because the tile tables in the data warehouse are now wiped out.
        await self.persistent.delete_many(
            collection_name=self.tile_registry_service.collection_name,
            query_filter={"feature_store_id": ObjectId(feature_store_id)},
            user_id=self.user.id,
        )
        await self.persistent.update_many(
            collection_name=self.deployed_tile_table_service.collection_name,
            query_filter={"feature_store_id": ObjectId(feature_store_id)},
            update={
                "$set": {
                    "backfill_metadata": None,
                    "last_run_metadata_online": None,
                    "last_run_metadata_offline": None,
                },
            },
            user_id=self.user.id,
        )

        # Initialize working schema. This covers registering tables, functions and procedures.
        initializer = session.initializer()
        if not initializer:
            return
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
        # activate use of raw query filter to retrieve all documents regardless of catalog membership
        with self.feature_service.allow_use_raw_query_filter():
            query_filter = {
                "tabular_source.feature_store_id": feature_store_id,
                "online_enabled": True,
            }
            online_enabled_features = self.feature_service.list_documents_iterator(
                query_filter=query_filter,
                use_raw_query_filter=True,
            )

            async for feature in online_enabled_features:
                logger.info(f"Rescheduling jobs for online enabled feature: {feature.name}")
                await OnlineEnableService.update_data_warehouse_with_session(
                    session=session,
                    feature_manager_service=self.feature_manager_service,
                    feature=feature,
                    target_online_enabled=True,
                )

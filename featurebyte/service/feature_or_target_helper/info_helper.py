"""
Info helper
"""
from bson import ObjectId

from featurebyte.enum import StrEnum
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.schema.info import BaseFeatureOrTargetTableInfo
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target_table import TargetTableService


class InfoType(StrEnum):
    HISTORICAL_FEATURE = "historical_feature"
    TARGET = "target"


class FeatureOrTargetInfoHelper:
    """
    Get HistoricalFeatureTable or TargetTable info helper
    """

    def __init__(
        self,
        target_table_service: TargetTableService,
        historical_feature_table_service: HistoricalFeatureTableService,
        observation_table_service: ObservationTableService,
    ):
        self.target_table_service = target_table_service
        self.historical_feature_table_service = historical_feature_table_service
        self.observation_table_service = observation_table_service

    async def get_info(
        self, document_id: ObjectId, info_type: InfoType
    ) -> BaseFeatureOrTargetTableInfo:
        """
        Get TargetTable info

        Parameters
        ----------
        document_id: ObjectId
            document id
        info_type: InfoType
            Info type

        Returns
        -------
        BaseFeatureOrTargetTableInfo

        Raises
        ------
        ValueError
        """
        if info_type == InfoType.HISTORICAL_FEATURE:
            document = await self.historical_feature_table_service.get_document(
                document_id=document_id
            )
        elif info_type == InfoType.TARGET:
            document = await self.target_table_service.get_document(document_id=document_id)
        else:
            raise ValueError(f"Invalid info type: {info_type}")
        assert isinstance(document, BaseFeatureOrTargetTableModel)

        if document.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=document.observation_table_id
            )
        else:
            observation_table = None
        return BaseFeatureOrTargetTableInfo(
            name=document.name,
            observation_table_name=observation_table.name if observation_table else None,
            table_details=document.location.table_details,
            created_at=document.created_at,
            updated_at=document.updated_at,
        )

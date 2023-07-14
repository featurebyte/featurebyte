"""
Feature or target table controller
"""
from typing import Any, TypeVar

from abc import abstractmethod

from bson import ObjectId

from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.routes.common.base import PaginatedDocument
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.schema.info import (
    BaseFeatureOrTargetTableInfo,
    HistoricalFeatureTableInfo,
    TargetTableInfo,
)
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.target_table import TargetTableService

InfoTypeT = TypeVar("InfoTypeT", HistoricalFeatureTableInfo, TargetTableInfo)

MaterializedTableDocumentT = TypeVar(
    "MaterializedTableDocumentT",
    HistoricalFeatureTableModel,
    TargetTableModel,
)
MaterializedTableDocumentServiceT = TypeVar(
    "MaterializedTableDocumentServiceT",
    HistoricalFeatureTableService,
    TargetTableService,
)


class FeatureOrTargetTableController(
    BaseMaterializedTableController[
        MaterializedTableDocumentT, MaterializedTableDocumentServiceT, PaginatedDocument
    ]
):
    """
    Feature or target table controller
    """

    info_class: type[InfoTypeT]

    def __init__(
        self,
        service: Any,
        preview_service: PreviewService,
        observation_table_service: ObservationTableService,
    ):
        super().__init__(service=service, preview_service=preview_service)
        self.observation_table_service = observation_table_service

    @abstractmethod
    async def get_additional_info_params(
        self, document: MaterializedTableDocumentT
    ) -> dict[str, Any]:
        """
        Get additional info params

        Parameters
        ----------
        document: MaterializedTableDocumentT
            document

        Returns
        -------
        dict[str, Any]
        """

    async def get_basic_info(
        self, document: BaseFeatureOrTargetTableModel
    ) -> BaseFeatureOrTargetTableInfo:
        """
        Get TargetTable info

        Parameters
        ----------
        document: BaseFeatureOrTargetTableModel

        Returns
        -------
        BaseFeatureOrTargetTableInfo

        Raises
        ------
        ValueError
        """
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

    async def get_info(self, document_id: ObjectId, verbose: bool) -> InfoTypeT:
        """
        Get HistoricalFeatureTable info

        Parameters
        ----------
        document_id: ObjectId
            document ID
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        InfoTypeT
        """
        _ = verbose
        document = await self.service.get_document(document_id)
        basic_info = await self.get_basic_info(document)
        additional_params = await self.get_additional_info_params(document)
        return self.info_class(
            **additional_params,
            **basic_info.dict(),
        )

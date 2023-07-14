"""
Feature or target table controller
"""
from typing import Any, TypeVar

from abc import abstractmethod

from bson import ObjectId

from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.routes.common.base import PaginatedDocument
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.schema.info import HistoricalFeatureTableInfo, TargetTableInfo
from featurebyte.service.feature_or_target_helper.info_helper import (
    FeatureOrTargetInfoHelper,
    InfoType,
)
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
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
    info_type: InfoType

    def __init__(
        self,
        service: Any,
        preview_service: PreviewService,
        feature_or_target_info_helper: FeatureOrTargetInfoHelper,
    ):
        super().__init__(service=service, preview_service=preview_service)
        self.feature_or_target_info_helper = feature_or_target_info_helper

    @abstractmethod
    async def get_additional_info_params(self, document_id: ObjectId) -> dict[str, Any]:
        """
        Get additional info params

        Parameters
        ----------
        document_id: ObjectId
            Feature or target table ID

        Returns
        -------
        dict[str, Any]
        """

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
        # TODO: maybe just pass the document in, and have children implement.
        basic_info = await self.feature_or_target_info_helper.get_info(
            document_id=document_id, info_type=self.info_type
        )
        additional_params = await self.get_additional_info_params(document_id=document_id)
        return self.info_class(
            **additional_params,
            **basic_info.dict(),
        )

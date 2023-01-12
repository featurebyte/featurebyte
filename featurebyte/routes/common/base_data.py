"""
BaseDataController for API routes
"""
from __future__ import annotations

from typing import Any, Type, TypeVar, cast

from abc import abstractmethod

from bson.objectid import ObjectId

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.tabular_data import DataServiceUpdate, DataUpdate
from featurebyte.service.data_update import DataDocumentService, DataUpdateService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.info import InfoService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService

DataDocumentT = TypeVar(
    "DataDocumentT", EventDataModel, ItemDataModel, DimensionDataModel, SCDDataModel
)
DataDocumentServiceT = TypeVar(
    "DataDocumentServiceT", EventDataService, ItemDataService, DimensionDataService, SCDDataService
)


class BaseDataDocumentController(
    BaseDocumentController[DataDocumentT, DataDocumentServiceT, PaginatedDocument]
):
    """
    BaseDataDocumentController for API routes
    """

    document_update_schema_class: Type[DataServiceUpdate]

    def __init__(
        self,
        service: DataDocumentService,
        data_update_service: DataUpdateService,
        semantic_service: SemanticService,
        info_service: InfoService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.data_update_service = data_update_service
        self.semantic_service = semantic_service
        self.info_service = info_service

    @abstractmethod
    async def _get_column_semantic_map(self, document: DataDocumentT) -> dict[str, Any]:
        """
        Construct column name to semantic mapping

        Parameters
        ----------
        document: DataDocumentT
            Newly created document

        Returns
        -------
        dict[str, Any]
        """

    async def _add_semantic_tags(self, document: DataDocumentT) -> DataDocumentT:
        """
        Add semantic tags to newly created document

        Parameters
        ----------
        document: DataDocumentT
            Newly created document

        Returns
        -------
        DataDocumentT
        """
        column_semantic_map = await self._get_column_semantic_map(document=document)
        columns_info = []
        for col_info in document.columns_info:
            semantic = column_semantic_map.get(col_info.name)
            if semantic:
                columns_info.append(ColumnInfo(**{**col_info.dict(), "semantic_id": semantic.id}))
            else:
                columns_info.append(col_info)

        output = await self.service.update_document(
            document_id=document.id,
            data=self.document_update_schema_class(columns_info=columns_info),  # type: ignore
            return_document=True,
        )
        return cast(DataDocumentT, output)

    async def create_data(self, data: DataDocumentT) -> DataDocumentT:
        """
        Create Data at persistent

        Parameters
        ----------
        data: DataDocumentT
            EventData or ItemData creation payload

        Returns
        -------
        DataDocumentT
            Newly created data object
        """
        document = await self.service.create_document(data)  # type: ignore[arg-type]
        return await self._add_semantic_tags(document=document)  # type: ignore

    async def update_data(self, document_id: ObjectId, data: DataUpdate) -> DataDocumentT:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Data document ID
        data: DataUpdate
            Data update payload

        Returns
        -------
        DataDocumentT
            Data object with updated attribute(s)
        """
        if data.columns_info:
            await self.data_update_service.update_columns_info(
                service=self.service,
                document_id=document_id,
                data=self.document_update_schema_class(**data.dict()),  # type: ignore
            )

        if data.status:
            await self.data_update_service.update_data_status(
                service=self.service,
                document_id=document_id,
                data=self.document_update_schema_class(**data.dict()),  # type: ignore
            )

        # update other parameters
        update_dict = data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True)
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=self.document_update_schema_class(**update_dict),  # type: ignore[arg-type]
                return_document=False,
            )

        return await self.get(document_id=document_id)

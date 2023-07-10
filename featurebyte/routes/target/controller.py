"""
Target controller
"""
from __future__ import annotations

from typing import Any, Dict, Literal, Optional

from http import HTTPStatus

from bson import ObjectId
from fastapi import HTTPException

from featurebyte.exception import MissingPointInTimeColumnError, RequiredEntityNotProvidedError
from featurebyte.models.target import TargetModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor
from featurebyte.schema.preview import FeatureOrTargetPreview
from featurebyte.schema.target import InputData, TableMetadata, TargetCreate, TargetInfo, TargetList
from featurebyte.service.entity import EntityService
from featurebyte.service.mixin import DEFAULT_PAGE_SIZE
from featurebyte.service.preview import PreviewService
from featurebyte.service.table import TableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService


class TargetController(BaseDocumentController[TargetModel, TargetService, TargetList]):
    """
    Target controller
    """

    paginated_document_class = TargetList

    def __init__(
        self,
        target_service: TargetService,
        target_namespace_service: TargetNamespaceService,
        entity_service: EntityService,
        preview_service: PreviewService,
        table_service: TableService,
        feature_or_target_metadata_extractor: FeatureOrTargetMetadataExtractor,
    ):
        super().__init__(target_service)
        self.target_namespace_service = target_namespace_service
        self.entity_service = entity_service
        self.preview_service = preview_service
        self.table_service = table_service
        self.feature_or_target_metadata_extractor = feature_or_target_metadata_extractor

    async def create_target(
        self,
        data: TargetCreate,
    ) -> TargetModel:
        """
        Create Target at persistent

        Parameters
        ----------
        data: TargetCreate
            Target creation payload

        Returns
        -------
        TargetModel
            Newly created Target object
        """
        return await self.service.create_document(data)

    async def list_target(
        self,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
        sort_by: Optional[str] = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: Optional[str] = None,
        name: Optional[str] = None,
    ) -> TargetList:
        """
        List Target at persistent

        Parameters
        ----------
        page: int
            Page number
        page_size: int
            Page size
        sort_by: str | None
            Key used to sort the returning documents
        sort_dir: "asc" or "desc"
            Sorting the returning documents in ascending order or descending order
        search: str | None
            Search token to be used in filtering
        name: str | None
            Name token to be used in filtering

        Returns
        -------
        TargetList
            List of Target objects
        """
        params: Dict[str, Any] = {"search": search, "name": name}
        return await self.list(
            page=page, page_size=page_size, sort_by=sort_by, sort_dir=sort_dir, **params
        )

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> TargetInfo:
        """
        Get target info given document ID.

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TargetInfo
        """
        _ = verbose
        target_doc = await self.service.get_document(document_id=document_id)
        namespace = await self.target_namespace_service.get_document(
            document_id=target_doc.target_namespace_id
        )
        entity_ids = target_doc.entity_ids or []
        entity_brief_info_list = await self.entity_service.get_entity_brief_info_list(
            set(entity_ids)
        )

        # Get input table metadata
        assert (
            len(target_doc.table_ids) == 1
        ), "Target should have only one table for now, until forward joins are supported."
        table_doc = await self.table_service.get_document(document_id=target_doc.table_ids[0])
        input_data = InputData(
            main_data=TableMetadata(
                name=table_doc.name,
                data_type=str(table_doc.type),
            ),
        )

        # Get metadata
        group_op_structure = target_doc.extract_operation_structure()
        target_metadata = await self.feature_or_target_metadata_extractor.extract(
            group_op_structure
        )

        return TargetInfo(
            id=document_id,
            target_name=target_doc.name,
            entities=entity_brief_info_list,
            horizon=namespace.horizon,
            has_recipe=bool(target_doc.graph),
            created_at=target_doc.created_at,
            updated_at=target_doc.updated_at,
            input_data=input_data,
            metadata=target_metadata,
        )

    async def preview(
        self, target_preview: FeatureOrTargetPreview, get_credential: Any
    ) -> dict[str, Any]:
        """
        Preview a Target

        Parameters
        ----------
        target_preview: FeatureOrTargetPreview
            Target preview payload
        get_credential: Any
            Get credential handler function

        Returns
        -------
        dict[str, Any]
            Dataframe converted to json string

        Raises
        ------
        HTTPException
            Invalid request payload
        """
        try:
            return await self.preview_service.preview_target_or_feature(
                feature_or_target_preview=target_preview, get_credential=get_credential
            )
        except (MissingPointInTimeColumnError, RequiredEntityNotProvidedError) as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=exc.args[0]
            ) from exc

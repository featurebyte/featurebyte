"""
DocumentInfoService class
"""
from __future__ import annotations

from typing import Any, Dict, Type

from bson.objectid import ObjectId

from featurebyte.persistent.base import Persistent
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService


class DocumentInfoService:
    """
    DocumentInfoService class
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    @property
    def document_service_map(self) -> Dict[str, Type[BaseDocumentService]]:
        """
        Collection name to service class mapping

        Returns
        -------
        Collection name key to service class value mapping
        """
        document_services = [
            EntityService,
            EventDataService,
            FeatureService,
            FeatureJobSettingAnalysisService,
            FeatureListService,
            FeatureNamespaceService,
            FeatureStoreService,
        ]
        return {
            document_service.document_class.collection_name(): document_service
            for document_service in document_services
        }

    async def _retrieve_reference(
        self,
        collection_name: str,
        foreign_key_value: list[ObjectId] | ObjectId,
    ) -> Any:
        if isinstance(foreign_key_value, list):
            return [
                await self.get_info(
                    collection_name=collection_name,
                    document_id=doc_id,
                    verbose=False,
                )
                for doc_id in foreign_key_value
            ]
        return await self.get_info(
            collection_name=collection_name, document_id=foreign_key_value, verbose=False
        )

    async def _populate_document(
        self,
        document: dict[str, Any],
        document_service: BaseDocumentService,
    ) -> dict[str, Any]:
        output = {}
        for key, value in document.items():
            if key in document_service.foreign_key_map and value is not None:
                # example: foreign_key_map = {"event_data_ids": "event_data"}
                # document contains "event_data_ids" key (list of event_data reference ids)
                # a new key called "event_data" will be inserted into the dictionary
                object_key = key.rsplit("_", 1)[0]
                output[object_key] = await self._retrieve_reference(
                    collection_name=document_service.foreign_key_map[key],
                    foreign_key_value=value,
                )
            if isinstance(value, dict):
                output[key] = await self._populate_document(
                    document=value, document_service=document_service
                )
            elif isinstance(value, list):
                output[key] = [
                    await self._populate_document(document=val, document_service=document_service)
                    if isinstance(val, dict)
                    else val
                    for val in value
                ]
            else:
                output[key] = value
        return output

    async def populate_document(
        self, collection_name: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Populate document by converting reference id value(s) to dictionary

        Parameters
        ----------
        collection_name: str
            Collection name
        document: dict[str, Any]
            Document dictionary
        Returns
        -------
        dict[str, Any]
        """
        document_service = self.document_service_map[collection_name](
            user=self.user, persistent=self.persistent
        )
        return await self._populate_document(document=document, document_service=document_service)

    async def get_info(
        self, collection_name: str, document_id: ObjectId, verbose: bool = True
    ) -> dict[str, Any]:
        """
        Construct info based on the given document_id

        Parameters
        ----------
        collection_name: str
            Collection name
        document_id: ObjectId
            ID of document to retrieve
        verbose: bool
            Control verbose level of the info
        Returns
        -------
        dict[str, Any]
        """
        document_service_class = self.document_service_map[collection_name]
        document = await document_service_class(
            user=self.user, persistent=self.persistent
        ).get_document(document_id=document_id)
        return document_service_class.info_transform.transform(
            input_value=await self.populate_document(
                collection_name=collection_name, document=document.dict()
            ),
            verbose=verbose,
        )

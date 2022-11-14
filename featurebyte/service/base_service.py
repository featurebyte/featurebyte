"""
BaseService class
"""
from __future__ import annotations

from typing import Any, Literal, Optional, Union, overload

from bson.objectid import ObjectId

from featurebyte.enum import StrEnum
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.entity import EntityModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature import FeatureModel, FeatureNamespaceModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListNamespaceModel
from featurebyte.models.feature_store import DataModel, FeatureStoreModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.models.semantic import SemanticModel
from featurebyte.persistent.base import Persistent
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.mixin import OpsServiceMixin
from featurebyte.service.scd_data import SCDDataService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.tabular_data import DataService

DocumentService = Union[
    DataService,
    FeatureStoreService,
    EntityService,
    SemanticService,
    EventDataService,
    DimensionDataService,
    SCDDataService,
    ItemDataService,
    FeatureService,
    FeatureNamespaceService,
    FeatureListService,
    FeatureListNamespaceService,
    FeatureJobSettingAnalysisService,
]
DocumentModel = Union[
    DataModel,
    FeatureStoreModel,
    EntityModel,
    SemanticModel,
    DimensionDataModel,
    EventDataModel,
    SCDDataModel,
    ItemDataModel,
    FeatureModel,
    FeatureNamespaceModel,
    FeatureListModel,
    FeatureListNamespaceModel,
    FeatureJobSettingAnalysisModel,
]


class DocServiceName(StrEnum):
    """
    Document service enum
    """

    DATA = "data"
    FEATURE_STORE = "feature_store"
    ENTITY = "entity"
    SEMANTIC = "semantic"
    DIMENSION_DATA = "dimension_data"
    EVENT_DATA = "event_data"
    ITEM_DATA = "item_data"
    SCD_DATA = "scd_data"
    FEATURE = "feature"
    FEATURE_NAMESPACE = "feature_namespace"
    FEATURE_LIST = "feature_list"
    FEATURE_LIST_NAMESPACE = "feature_list_namespace"
    FEATURE_JOB_SETTING_ANALYSIS = "feature_job_setting_analysis"


class BaseService(OpsServiceMixin):
    """
    BaseService class has access to all document services as property.
    """

    def __init__(self, user: Any, persistent: Persistent):
        self.user = user
        self.persistent = persistent

    @property
    def data_service(self) -> DataService:
        """
        DataService object

        Returns
        -------
        DataService
        """
        return DataService(user=self.user, persistent=self.persistent)

    @property
    def feature_store_service(self) -> FeatureStoreService:
        """
        FeatureStoreService object

        Returns
        -------
        FeatureStoreService
        """
        return FeatureStoreService(user=self.user, persistent=self.persistent)

    @property
    def entity_service(self) -> EntityService:
        """
        EntityService object

        Returns
        -------
        EntityService
        """
        return EntityService(user=self.user, persistent=self.persistent)

    @property
    def semantic_service(self) -> SemanticService:
        """
        SemanticService object

        Returns
        -------
        SemanticService
        """
        return SemanticService(user=self.user, persistent=self.persistent)

    @property
    def dimension_data_service(self) -> DimensionDataService:
        """
        DimensionDataService object

        Returns
        -------
        DimensionDataService
        """
        return DimensionDataService(user=self.user, persistent=self.persistent)

    @property
    def scd_data_service(self) -> SCDDataService:
        """
        SCDDataService object

        Returns
        -------
        SCDDataService
        """
        return SCDDataService(user=self.user, persistent=self.persistent)

    @property
    def event_data_service(self) -> EventDataService:
        """
        EventDataService object

        Returns
        -------
        EventDataService
        """
        return EventDataService(user=self.user, persistent=self.persistent)

    @property
    def item_data_service(self) -> ItemDataService:
        """
        ItemDataService object

        Returns
        -------
        ItemDataService
        """
        return ItemDataService(user=self.user, persistent=self.persistent)

    @property
    def feature_service(self) -> FeatureService:
        """
        FeatureService object

        Returns
        -------
        FeatureService
        """
        return FeatureService(user=self.user, persistent=self.persistent)

    @property
    def feature_namespace_service(self) -> FeatureNamespaceService:
        """
        FeatureNamespaceService object

        Returns
        -------
        FeatureNamespaceService
        """
        return FeatureNamespaceService(user=self.user, persistent=self.persistent)

    @property
    def feature_list_service(self) -> FeatureListService:
        """
        FeatureListService object

        Returns
        -------
        FeatureListService
        """
        return FeatureListService(user=self.user, persistent=self.persistent)

    @property
    def feature_list_namespace_service(self) -> FeatureListNamespaceService:
        """
        FeatureListNamespaceService object

        Returns
        -------
        FeatureListNamespaceService
        """
        return FeatureListNamespaceService(user=self.user, persistent=self.persistent)

    @property
    def feature_job_setting_analysis_service(self) -> FeatureJobSettingAnalysisService:
        """
        FeatureJobSettingAnalysisService object

        Returns
        -------
        FeatureJobSettingAnalysisService
        """
        return FeatureJobSettingAnalysisService(user=self.user, persistent=self.persistent)

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.DATA]) -> DataService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.FEATURE_STORE]
    ) -> FeatureStoreService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.ENTITY]) -> EntityService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.SEMANTIC]) -> SemanticService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.DIMENSION_DATA]
    ) -> DimensionDataService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.SCD_DATA]) -> SCDDataService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.EVENT_DATA]) -> EventDataService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.ITEM_DATA]) -> ItemDataService:
        ...

    @overload
    def as_service(self, doc_service_name: Literal[DocServiceName.FEATURE]) -> FeatureService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.FEATURE_NAMESPACE]
    ) -> FeatureNamespaceService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.FEATURE_LIST]
    ) -> FeatureListService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.FEATURE_LIST_NAMESPACE]
    ) -> FeatureListNamespaceService:
        ...

    @overload
    def as_service(
        self, doc_service_name: Literal[DocServiceName.FEATURE_JOB_SETTING_ANALYSIS]
    ) -> FeatureJobSettingAnalysisService:
        ...

    def as_service(self, doc_service_name: DocServiceName) -> BaseDocumentService:
        """
        Convert document service name to document service object

        Parameters
        ----------
        doc_service_name: DocServiceName
            Document service name

        Returns
        -------
        DocumentServiceT
        """
        doc_service_name_map: dict[DocServiceName, BaseDocumentService] = {
            DocServiceName.DATA: self.data_service,
            DocServiceName.FEATURE_STORE: self.feature_store_service,
            DocServiceName.ENTITY: self.entity_service,
            DocServiceName.SEMANTIC: self.semantic_service,
            DocServiceName.DIMENSION_DATA: self.dimension_data_service,
            DocServiceName.SCD_DATA: self.scd_data_service,
            DocServiceName.EVENT_DATA: self.event_data_service,
            DocServiceName.ITEM_DATA: self.item_data_service,
            DocServiceName.FEATURE: self.feature_service,
            DocServiceName.FEATURE_NAMESPACE: self.feature_namespace_service,
            DocServiceName.FEATURE_LIST: self.feature_list_service,
            DocServiceName.FEATURE_LIST_NAMESPACE: self.feature_list_namespace_service,
            DocServiceName.FEATURE_JOB_SETTING_ANALYSIS: self.feature_job_setting_analysis_service,
        }
        return doc_service_name_map[doc_service_name]

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.DATA],
        document_id: ObjectId,
        document: Optional[DataModel] = None,
    ) -> DataModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE_STORE],
        document_id: ObjectId,
        document: Optional[FeatureStoreModel] = None,
    ) -> FeatureStoreModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.ENTITY],
        document_id: ObjectId,
        document: Optional[EntityModel] = None,
    ) -> EntityModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.SEMANTIC],
        document_id: ObjectId,
        document: Optional[SemanticModel] = None,
    ) -> SemanticModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.EVENT_DATA],
        document_id: ObjectId,
        document: Optional[EventDataModel] = None,
    ) -> EventDataModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.DIMENSION_DATA],
        document_id: ObjectId,
        document: Optional[DimensionDataModel] = None,
    ) -> DimensionDataModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.SCD_DATA],
        document_id: ObjectId,
        document: Optional[SCDDataModel] = None,
    ) -> SCDDataModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.ITEM_DATA],
        document_id: ObjectId,
        document: Optional[ItemDataModel] = None,
    ) -> ItemDataModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE],
        document_id: ObjectId,
        document: Optional[FeatureModel] = None,
    ) -> FeatureModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE_NAMESPACE],
        document_id: ObjectId,
        document: Optional[FeatureNamespaceModel] = None,
    ) -> FeatureNamespaceModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE_LIST],
        document_id: ObjectId,
        document: Optional[FeatureListModel] = None,
    ) -> FeatureListModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE_LIST_NAMESPACE],
        document_id: ObjectId,
        document: Optional[FeatureListNamespaceModel] = None,
    ) -> FeatureListNamespaceModel:
        ...

    @overload
    async def get_document(
        self,
        doc_service_name: Literal[DocServiceName.FEATURE_JOB_SETTING_ANALYSIS],
        document_id: ObjectId,
        document: Optional[FeatureJobSettingAnalysisModel] = None,
    ) -> FeatureJobSettingAnalysisModel:
        ...

    async def get_document(
        self,
        doc_service_name: Literal[
            DocServiceName.DATA,
            DocServiceName.FEATURE_STORE,
            DocServiceName.ENTITY,
            DocServiceName.SEMANTIC,
            DocServiceName.DIMENSION_DATA,
            DocServiceName.SCD_DATA,
            DocServiceName.EVENT_DATA,
            DocServiceName.ITEM_DATA,
            DocServiceName.FEATURE,
            DocServiceName.FEATURE_NAMESPACE,
            DocServiceName.FEATURE_LIST,
            DocServiceName.FEATURE_LIST_NAMESPACE,
            DocServiceName.FEATURE_JOB_SETTING_ANALYSIS,
        ],
        document_id: ObjectId,
        document: Optional[DocumentModel] = None,
    ) -> DocumentModel:
        """
        Retrieve document given document service name and document ID if the document is empty

        Parameters
        ----------
        doc_service_name: DocumentServiceName
            Document service name
        document_id: ObjectId
            Document ID
        document: Optional[DocumentT]
            Document object

        Returns
        -------
        DocumentT
        """
        if document is None:
            document = await self.as_service(doc_service_name).get_document(document_id=document_id)
        return document

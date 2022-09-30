"""
FeatureStoreService class
"""
from __future__ import annotations

from typing import Any, List, Optional, Type

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ColumnSpec, FeatureStoreModel
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreInfo
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin


class FeatureStoreService(
    BaseDocumentService[FeatureStoreModel], GetInfoServiceMixin[FeatureStoreInfo]
):
    """
    FeatureStoreService class
    """

    document_class: Type[FeatureStoreModel] = FeatureStoreModel

    async def create_document(  # type: ignore[override]
        self, data: FeatureStoreCreate, get_credential: Any = None
    ) -> FeatureStoreModel:
        _ = get_credential
        document = FeatureStoreModel(**data.json_dict(), user_id=self.user.id)

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: FeatureByteBaseModel,
        exclude_none: bool = True,
        document: Optional[FeatureStoreModel] = None,
        return_document: bool = True,
    ) -> Optional[FeatureStoreModel]:
        # TODO: implement proper logic to update feature store document
        if document is None:
            document = await self.get_document(document_id=document_id)

        if return_document:
            return document
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> FeatureStoreInfo:
        _ = verbose
        feature_store = await self.get_document(document_id=document_id)
        return FeatureStoreInfo(
            name=feature_store.name,
            created_at=feature_store.created_at,
            updated_at=feature_store.updated_at,
            source=feature_store.type,
            database_details=feature_store.details,
        )

    async def list_databases(
        self, feature_store: FeatureStoreModel, get_credential: Any
    ) -> List[str]:
        """
        List databases in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of database names
        """
        db_session = await self._get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_databases()

    async def list_schemas(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        get_credential: Any,
    ) -> List[str]:
        """
        List schemas in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of schema names
        """
        db_session = await self._get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_schemas(database_name=database_name)

    async def list_tables(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        get_credential: Any,
    ) -> List[str]:
        """
        List tables in feature store

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[str]
            List of table names
        """
        db_session = await self._get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        return await db_session.list_tables(database_name=database_name, schema_name=schema_name)

    async def list_columns(
        self,
        feature_store: FeatureStoreModel,
        database_name: str,
        schema_name: str,
        table_name: str,
        get_credential: Any,
    ) -> List[ColumnSpec]:
        """
        List columns in database table

        Parameters
        ----------
        feature_store: FeatureStoreModel
            FeatureStoreModel object
        database_name: str
            Name of database to use
        schema_name: str
            Name of schema to use
        table_name: str
            Name of table to use
        get_credential: Any
            Get credential handler function

        Returns
        -------
        List[ColumnSpec]
            List of ColumnSpec object
        """
        db_session = await self._get_feature_store_session(
            feature_store=feature_store, get_credential=get_credential
        )
        table_schema = await db_session.list_table_schema(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return [ColumnSpec(name=name, dtype=dtype) for name, dtype in table_schema.items()]

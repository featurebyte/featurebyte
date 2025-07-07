"""
DevelopmentDatasetService class
"""

from __future__ import annotations

from typing import Any, Optional, Type, cast

from bson import ObjectId
from redis import Redis

from featurebyte.exception import (
    FeatureStoreNotInCatalogError,
    InvalidTableSchemaError,
)
from featurebyte.models import FeatureStoreModel
from featurebyte.models.development_dataset import DevelopmentDatasetModel, DevelopmentTable
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.schema.development_dataset import (
    DevelopmentDatasetCreate,
    DevelopmentDatasetUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.table import TableService
from featurebyte.storage import Storage


class DevelopmentDatasetService(
    BaseDocumentService[
        DevelopmentDatasetModel, DevelopmentDatasetCreate, BaseDocumentServiceUpdateSchema
    ],
):
    """
    DevelopmentDatasetService class
    """

    document_class: Type[DevelopmentDatasetModel] = DevelopmentDatasetModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        catalog_service: CatalogService,
        table_service: TableService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        block_modification_handler: BlockModificationHandler,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.catalog_service = catalog_service
        self.table_service = table_service
        self.feature_store_service = feature_store_service
        self.feature_store_warehouse_service = feature_store_warehouse_service

    async def _validate_development_tables(
        self, development_tables: list[DevelopmentTable]
    ) -> None:
        """
        Validate development tables

        Parameters
        ----------
        development_tables: list[DevelopmentTable]
            List of development tables to validate.

        Raises
        ------
        FeatureStoreNotInCatalogError
            If the feature store is not part of the catalog.
        InvalidTableSchemaError
            If the table schema is invalid.
        """
        # check that feature store matches the catalog
        assert self.catalog_id is not None
        catalog = await self.catalog_service.get_document(document_id=self.catalog_id)
        for dev_table in development_tables:
            if dev_table.location.feature_store_id not in catalog.default_feature_store_ids:
                raise FeatureStoreNotInCatalogError(
                    f'Feature store "{dev_table.location.feature_store_id}" does not belong to catalog '
                    f'"{self.catalog_id}".'
                )

        # validate columns of replacement tables include all columns from original table
        feature_store_id_to_doc: dict[ObjectId, FeatureStoreModel] = {}
        for dev_table in development_tables:
            feature_store = feature_store_id_to_doc.get(dev_table.location.feature_store_id)
            if feature_store is None:
                feature_store = await self.feature_store_service.get_document(
                    document_id=dev_table.location.feature_store_id,
                )
                feature_store_id_to_doc[dev_table.location.feature_store_id] = feature_store

            # get source and destination column information
            table = await self.table_service.get_document(document_id=dev_table.table_id)
            source_column_specs = {col_info.name: col_info for col_info in table.columns_info}
            source_info = feature_store.get_source_info()
            dest_column_info = await self.feature_store_warehouse_service.list_columns(
                feature_store=feature_store,
                database_name=dev_table.location.table_details.database_name
                or source_info.database_name,
                schema_name=dev_table.location.table_details.schema_name or source_info.schema_name,
                table_name=dev_table.location.table_details.table_name,
            )
            dest_column_specs = {col_info.name: col_info for col_info in dest_column_info}

            # ensure all required columns are present in the destination
            missing_columns = set(source_column_specs.keys()) - set(dest_column_specs.keys())
            if missing_columns:
                raise InvalidTableSchemaError(
                    f'Development source for table "{table.name}" missing required columns: {", ".join(sorted(list(missing_columns)))}'
                )

            # ensure column types match
            mismatch_columns = [
                f"{column_name} (expected {required_specs.dtype}, got {dest_column_specs[column_name].dtype})"
                for column_name, required_specs in source_column_specs.items()
                if not dest_column_specs[column_name].is_compatible_with(required_specs)
            ]
            if mismatch_columns:
                raise InvalidTableSchemaError(
                    f'Development source for table "{table.name}" column type mismatch: {", ".join(mismatch_columns)}'
                )

    async def create_document(self, data: DevelopmentDatasetCreate) -> DevelopmentDatasetModel:
        await self._validate_development_tables(data.development_tables)
        return await super().create_document(data=data)

    async def update_document(
        self,
        document_id: ObjectId,
        data: BaseDocumentServiceUpdateSchema,
        exclude_none: bool = True,
        document: Optional[DevelopmentDatasetModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
        populate_remote_attributes: bool = True,
    ) -> Optional[DevelopmentDatasetModel]:
        original_document = await self.get_document(
            document_id=document_id, populate_remote_attributes=False
        )
        assert isinstance(data, DevelopmentDatasetUpdate)
        if data.development_tables is not None:
            data.development_tables = original_document.development_tables + data.development_tables
            await self._validate_development_tables(data.development_tables)

        return cast(
            DevelopmentDatasetModel,
            await super().update_document(
                document_id=document_id,
                data=data,
                exclude_none=exclude_none,
                document=original_document,
                return_document=return_document,
                skip_block_modification_check=skip_block_modification_check,
                populate_remote_attributes=populate_remote_attributes,
            ),
        )

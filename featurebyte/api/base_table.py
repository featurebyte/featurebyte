"""
DataColumn class
"""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from typing import Any, ClassVar, List, Optional, Tuple, Type, TypeVar, Union, cast

import pandas as pd
from bson import ObjectId
from pandas import DataFrame
from pydantic import Field
from typeguard import typechecked
from typing_extensions import Literal

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.api.source_table import AbstractTableData, SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.core.mixin import GetAttrMixin, ParentMixin
from featurebyte.enum import TableDataType, ViewMode
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel, TableStatus
from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.cleaning_operation import (
    CleaningOperation,
    ColumnCleaningOperation,
)
from featurebyte.query_graph.sql.interpreter import GraphInterpreter

SourceTableApiObjectT = TypeVar("SourceTableApiObjectT", bound="TableApiObject")
TableDataT = TypeVar("TableDataT", bound=BaseTableData)


class TableColumn(FeatureByteBaseModel, ParentMixin):
    """
    TableColumn class that is used to set metadata such as Entity column. It holds a reference to its
    parent, which is a table object (e.g. EventTable)
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TableColumn")

    # pydantic instance variable (public)
    name: str

    @property
    def info(self) -> ColumnInfo:
        """
        Column information which contains column name, column type, associated entity ID & associated
        semantic ID.

        Returns
        -------
        ColumnInfo
        """
        column_info = next(col for col in self.parent.columns_info if col.name == self.name)
        return cast(ColumnInfo, column_info)

    @property
    def description(self) -> Optional[str]:
        """
        Description of the column.

        Returns
        -------
        Optional[str]
        """
        return self.info.description

    @property
    def cleaning_operations(self) -> List[CleaningOperation]:
        """
        Cleaning operations applied to the column of the table.

        Returns
        -------
        List[CleaningOperation]
            List of cleaning operations applied to the column of the table.

        Examples
        --------
        Show the list of cleaning operations of the event table amount column after updating the critical
        data info.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> event_table["Amount"].cleaning_operations
        [MissingValueImputation(imputed_value=0.0)]

        Empty list of column cleaning operations of the event table amount column.

        >>> event_table["Amount"].update_critical_data_info(cleaning_operations=[])
        >>> event_table["Amount"].cleaning_operations
        []
        """
        for column_cleaning_operations in self.parent.column_cleaning_operations:
            if column_cleaning_operations.column_name == self.name:
                return cast(List[CleaningOperation], column_cleaning_operations.cleaning_operations)
        return []

    @property
    def feature_store(self) -> FeatureStoreModel:
        """
        Feature store used by parent frame

        Returns
        -------
        FeatureStoreModel
        """
        return cast(FeatureStoreModel, self.parent.feature_store)

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Tags an entity to a column that represents the entity in the table, providing further context and organization
        to facilitate feature engineering.

        Parameters
        ----------
        entity_name: Optional[str]
            The name of the entity that is represented or referenced by the column. If None, removes prior association.

        Examples
        --------
        Set column "GroceryInvoiceGuid" of the target event table as entity "groceryinvoice"

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["GroceryInvoiceGuid"].as_entity("groceryinvoice")
        """
        self.parent.update_column_entity(self.info.name, entity_name)

    @typechecked
    def update_critical_data_info(self, cleaning_operations: List[CleaningOperation]) -> None:
        """
        Associates metadata with the column such as default cleaning operations that automatically apply when views
        are created from a table. These operations help ensure data consistency and accuracy.

        For a specific column, define a sequence of cleaning operations to be executed in order. Ensure that values
        imputed in earlier steps are not marked for cleaning in subsequent operations.

        To set default cleaning operations for a column, use the following class objects in a list:

        * `MissingValueImputation`: Imputes missing values.
        * `DisguisedValueImputation`: Imputes disguised values from a list.
        * `UnexpectedValueImputation`: Imputes unexpected values not found in a given list.
        * `ValueBeyondEndpointImputation`: Imputes numeric or date values outside specified boundaries.
        * `StringValueImputation`: Imputes string values.
        * `AddTimestampSchema`: Adds timestamp related metadata to a column.

        If the `imputed_value` parameter is None, the values to impute are replaced with missing values and the
        corresponding rows are ignored during aggregation operations.

        Parameters
        ----------
        cleaning_operations: List[CleaningOperation]
            List of cleaning operations to be applied on the column.

        Examples
        --------
        Add missing value imputation & negative value imputation operations to a table column.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...         fb.ValueBeyondEndpointImputation(
        ...             type="less_than", end_point=0, imputed_value=0
        ...         ),
        ...     ]
        ... )

        Show column cleaning operations of the event table.

        >>> event_table.column_cleaning_operations
        [ColumnCleaningOperation(column_name='Amount', cleaning_operations=[MissingValueImputation(imputed_value=0.0),
        ValueBeyondEndpointImputation(type=less_than, imputed_value=0.0, end_point=0.0)])]

        Remove cleaning operations and show the column cleaning operations of the event table.

        >>> event_table["Amount"].update_critical_data_info(cleaning_operations=[])
        >>> event_table.column_cleaning_operations
        []

        See Also
        --------
        - [Table.columns_info](/reference/featurebyte.api.base_table.TableApiObject.columns_info)
        - [Table.column_cleaning_operations](/reference/featurebyte.api.base_table.TableApiObject.column_cleaning_operations)
        """
        critical_data_info = CriticalDataInfo(cleaning_operations=cleaning_operations)
        self.parent.update_column_critical_data_info(self.info.name, critical_data_info)

    @typechecked
    def update_description(self, description: Optional[str]) -> None:
        """
        Update description of a column in the table.

        Parameters
        ----------
        description: Optional[str]
            The description of the column.

        Examples
        --------
        Update description of column "GroceryInvoiceGuid" of the target event table

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["GroceryInvoiceGuid"].update_description("Invoice ID")
        """
        self.parent.update_column_description(self.info.name, description)

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        tuple[QueryGraphModel, Node]
            QueryGraph & mapped Node object (within the pruned graph)
        """
        pruned_graph, mapped_node = self.parent.frame.extract_pruned_graph_and_node(**kwargs)
        project_node = pruned_graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [self.info.name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[mapped_node],
        )
        return pruned_graph, project_node

    @typechecked
    def preview_sql(self, limit: int = 10, after_cleaning: bool = False) -> str:
        """
        Returns an SQL query for previewing the column data.

        Parameters
        ----------
        limit: int
            Maximum number of return rows
        after_cleaning: bool
            Whether to preview the table after cleaning

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(
            after_cleaning=after_cleaning
        )
        return GraphInterpreter(
            pruned_graph, source_info=self.feature_store.get_source_info()
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the table column. By default, the materialization
        process occurs before any cleaning operations that were defined at the table level.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table column.

        Examples
        --------
        Preview a table without cleaning operations.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> description = event_table.preview(limit=5)


        Preview a table after cleaning operations have been applied.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> description = event_table.preview(limit=5, after_cleaning=True)
        """
        return self.parent.preview(limit=limit, after_cleaning=after_cleaning)[[self.info.name]]

    @typechecked
    def shape(self, after_cleaning: bool = False) -> Tuple[int, int]:
        """
        Return the shape of the table column.

        Parameters
        ----------
        after_cleaning: bool
            Whether to get the shape of the column after cleaning

        Returns
        -------
        Tuple[int, int]

        Examples
        --------
        Get the shape of a column.
        >>> catalog.get_table("INVOICEITEMS")["Quantity"].shape()
        (300450, 1)
        """
        return self.parent.shape(after_cleaning=after_cleaning)[0], 1

    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
    ) -> pd.DataFrame:
        """
        Returns a Series that contains a random selection of rows of the table column based on a specified time range,
        size, and seed for sampling control. By default, the materialization process occurs before any cleaning
        operations that were defined at the column level.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to sample the table after cleaning

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table column.

        Examples
        --------
        Sample 3 rows from the table.
        >>> sample = catalog.get_table("GROCERYINVOICE")["Amount"].sample(3)


        Sample 3 rows from the table with timestamps after cleaning operations have been applied.
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> event_table["Amount"].sample(  # doctest: +SKIP
        ...     size=3,
        ...     seed=111,
        ...     from_timestamp=datetime(2019, 1, 1),
        ...     to_timestamp=datetime(2023, 12, 31),
        ...     after_cleaning=True,
        ... )
        """
        return self.parent.sample(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )[[self.info.name]]

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
    ) -> pd.DataFrame:
        """
        Returns descriptive statistics of the table column. By default, the statistics are computed before any
        cleaning operations that were defined at the table level.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to compute description statistics after cleaning.

        Returns
        -------
        pd.DataFrame
            Summary of the table column.

        Examples
        --------
        Describe a table without cleaning operations

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> description = event_table["Amount"].describe(
        ...     from_timestamp=datetime(2020, 1, 1),
        ...     to_timestamp=datetime(2020, 1, 31),
        ... )


        Describe a table after cleaning operations have been applied.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> description = event_table["Amount"].describe(
        ...     from_timestamp=datetime(2020, 1, 1),
        ...     to_timestamp=datetime(2020, 1, 31),
        ...     after_cleaning=True,
        ... )
        """
        return self.parent.describe(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )[[self.info.name]]


class TableListMixin(ApiObject):
    """
    Mixin to implement source table list function
    """

    # class variables
    _route: ClassVar[str] = "/table"
    _list_schema: ClassVar[Any] = ProxyTableModel
    _list_fields: ClassVar[List[str]] = ["name", "type", "status", "entities", "created_at"]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("columns_info.entity_id", Entity, "entities"),
    ]

    @classmethod
    def list(cls, include_id: Optional[bool] = False, entity: Optional[str] = None) -> DataFrame:
        """
        List saved table sources

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        entity: Optional[str]
            Name of entity used to filter results

        Returns
        -------
        DataFrame
            Table of table sources
        """
        data_list = super().list(include_id=include_id)
        if entity:
            data_list = data_list[data_list.entities.apply(lambda entities: entity in entities)]
        return data_list


class TableApiObject(
    GetAttrMixin, AbstractTableData, TableListMixin, DeletableApiObject, SavableApiObject
):
    """
    Base class for all Table objects
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Table")
    _create_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None

    # pydantic instance variables
    type: Literal[
        TableDataType.SOURCE_TABLE,
        TableDataType.EVENT_TABLE,
        TableDataType.ITEM_TABLE,
        TableDataType.DIMENSION_TABLE,
        TableDataType.SCD_TABLE,
        TableDataType.TIME_SERIES_TABLE,
    ] = Field(
        description="Table type. Either source_table, event_table, item_table, dimension_table, scd_table or time_series_table."
    )

    # pydantic instance variable (internal use)
    internal_record_creation_timestamp_column: Optional[str] = Field(
        alias="record_creation_timestamp_column"
    )

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        Returns the list of entity IDs that are represented in the table.

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.entity_ids

    @property
    def table_data(self) -> BaseTableData:
        """
        Table data object of this table. This object contains information used to construct the SQL query.

        Returns
        -------
        BaseTableData
            Table data object used for SQL query construction.
        """
        try:
            return self._table_data_class(**self.cached_model.model_dump(by_alias=True))
        except RecordRetrievalException:
            return self._table_data_class(**self.model_dump(by_alias=True))

    @property
    def columns_info(self) -> List[ColumnInfo]:
        """
        Provides information about the columns in the table such as column name, column type, entity ID associated
        with the column, semantic ID associated with the column, and the critical data information associated with
        the column.

        Returns
        -------
        List[ColumnInfo]
            List of column information.

        Examples
        --------
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> column_info = event_table.columns_info

        See Also
        --------
        - [Table.column_cleaning_operations](/reference/featurebyte.api.base_table.TableApiObject.column_cleaning_operations)
        - [TableColumn.update_critical_data_info](/reference/featurebyte.api.base_table.TableColumn.update_critical_data_info)
        """
        try:
            return self.cached_model.columns_info
        except RecordRetrievalException:
            return self.internal_columns_info

    @property
    def catalog_id(self) -> ObjectId:
        """
        Returns the unique identifier (ID) of the Catalog that is associated with the Table object.

        Returns
        -------
        ObjectId
            Catalog ID of the table.

        See Also
        --------
        - [Catalog](/reference/featurebyte.api.catalog.Catalog)
        """
        return self.cached_model.catalog_id

    @property
    def primary_key_columns(self) -> List[str]:
        """
        List of primary key columns of the table.

        Returns
        -------
        List[str]
            List of primary key columns
        """
        return self.cached_model.primary_key_columns

    @property
    def status(self) -> TableStatus:
        """
        Returns the status of the Table object. A table can be categorized into 3 status: PUBLIC DRAFT, PUBLISHED, and
        DEPRECATED.

        Once a table status is DEPRECATED, you can create a new table using the same source table.

        Returns
        -------
        TableStatus
            Table status
        """
        try:
            return self.cached_model.status
        except RecordRetrievalException:
            return TableStatus.PUBLIC_DRAFT

    @property
    def record_creation_timestamp_column(self) -> Optional[str]:
        """
        Returns the name of the column in the table that represents the timestamp for record creation. This
        information is utilized to analyze feature job settings, and the identified column is treated as a special
        column that is not meant for feature engineering. By default, any view created from the table will exclude
        this column.

        Returns
        -------
        Optional[str]
            Record creation timestamp column name
        """
        try:
            return self.cached_model.record_creation_timestamp_column
        except RecordRetrievalException:
            return self.internal_record_creation_timestamp_column

    @property
    def column_cleaning_operations(self) -> List[ColumnCleaningOperation]:
        """
        List of column cleaning operations associated with this table. Column cleaning operation is a list of
        cleaning operations to be applied to a column of this table.

        Returns
        -------
        List[ColumnCleaningOperation]
            List of column cleaning operations

        Examples
        --------
        Show the list of column cleaning operations of an event table.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...         fb.ValueBeyondEndpointImputation(
        ...             type="less_than", end_point=0, imputed_value=0
        ...         ),
        ...     ]
        ... )
        >>> event_table.column_cleaning_operations
        [ColumnCleaningOperation(column_name='Amount', cleaning_operations=[MissingValueImputation(imputed_value=0.0), ValueBeyondEndpointImputation(type=less_than, imputed_value=0.0, end_point=0.0)])]

        Empty list of column cleaning operations after resetting the cleaning operations.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(cleaning_operations=[])
        >>> event_table.column_cleaning_operations
        []


        See Also
        --------
        - [Table.columns_info](/reference/featurebyte.api.base_table.TableApiObject.columns_info)
        - [TableColumn.update_critical_data_info](/reference/featurebyte.api.base_table.TableColumn.update_critical_data_info)
        """
        return [
            ColumnCleaningOperation(
                column_name=col.name, cleaning_operations=col.critical_data_info.cleaning_operations
            )
            for col in self.columns_info
            if col.critical_data_info is not None and col.critical_data_info.cleaning_operations
        ]

    def _get_create_payload(self) -> dict[str, Any]:
        assert self._create_schema_class is not None
        data = self._create_schema_class(**self.model_dump(by_alias=True))
        return data.json_dict()

    @classmethod
    @typechecked
    def create(
        cls: Type[SourceTableApiObjectT],
        source_table: SourceTable,
        name: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
        **kwargs: Any,
    ) -> SourceTableApiObjectT:
        """
        Create derived instances of TableApiObject from tabular source

        Parameters
        ----------
        source_table: SourceTable
            Source table object used to construct the table
        name: str
            Object name
        record_creation_timestamp_column: str
            Record creation timestamp column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object
        **kwargs: Any
            Additional parameters specific to variants of TableApiObject

        Returns
        -------
        SourceTableApiObjectT

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        assert cls._create_schema_class is not None

        data_id_value = _id or ObjectId()
        data = cls._create_schema_class(
            _id=data_id_value,
            name=name,
            tabular_source=source_table.tabular_source,
            columns_info=source_table.columns_info,
            record_creation_timestamp_column=record_creation_timestamp_column,
            **kwargs,
        )

        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                table = cls(
                    **data.model_dump(by_alias=True),
                    feature_store=source_table.feature_store,
                    _validate_schema=True,
                )
                assert table.id == data_id_value
                table.save()
                assert table.id == data_id_value
                return table
            existing_record = response_dict["data"][0]
            raise DuplicatedRecordException(
                response,
                f'{cls.__name__} ({existing_record["type"]}.name: "{name}") exists in saved record.',
            )
        raise RecordRetrievalException(response)

    @classmethod
    @typechecked
    def get_or_create(
        cls: Type[SourceTableApiObjectT],
        source_table: SourceTable,
        name: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
        **kwargs: Any,
    ) -> SourceTableApiObjectT:
        """
        Get or derived instances of TableApiObject from tabular source. Internally, this method calls `get`
        by table name first, if the table is not found, it will call `create` to create a new table.

        Parameters
        ----------
        source_table: SourceTable
            Source table object used to construct the table
        name: str
            Object name
        record_creation_timestamp_column: str
            Record creation timestamp column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object
        **kwargs: Any
            Additional parameters specific to variants of TableApiObject

        Returns
        -------
        SourceTableApiObjectT
        """
        try:
            return cls.get(name=name)
        except RecordRetrievalException:
            return cls.create(
                source_table=source_table,
                name=name,
                record_creation_timestamp_column=record_creation_timestamp_column,
                _id=_id,
                **kwargs,
            )

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store}

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
    ) -> pd.DataFrame:
        """
        Returns descriptive statistics of the table. By default, the statistics are computed before any cleaning
        operations that were defined at the table level.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Summary of the table.

        Examples
        --------
        Describe a table without cleaning operations
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> description = event_table.describe(
        ...     from_timestamp=datetime(2020, 1, 1),
        ...     to_timestamp=datetime(2020, 1, 31),
        ... )


        Describe a table after cleaning operations have been applied.
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> description = event_table.describe(
        ...     from_timestamp=datetime(2020, 1, 1),
        ...     to_timestamp=datetime(2020, 1, 31),
        ...     after_cleaning=True,
        ... )
        """
        return super().describe(size, seed, from_timestamp, to_timestamp, after_cleaning)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the table. By default, the materialization process
        occurs before any cleaning operations that were defined at the table level.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        Preview a table without cleaning operations.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> description = event_table.preview(limit=5)


        Preview a table after cleaning operations have been applied.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> description = event_table.preview(limit=5, after_cleaning=True)
        """
        return super().preview(limit=limit, after_cleaning=after_cleaning)

    @typechecked
    def __getitem__(self, item: str) -> TableColumn:
        """
        Retrieve column from the table

        Parameters
        ----------
        item: str
            Column name

        Returns
        -------
        TableColumn

        Raises
        ------
        KeyError
            when accessing non-exist column
        """
        info = None
        for col in self.columns_info:
            if col.name == item:
                info = col
        if info is None:
            raise KeyError(f'Column "{item}" does not exist!')
        output = TableColumn(name=item)
        output.set_parent(self)
        return output

    @typechecked
    def update_record_creation_timestamp_column(
        self, record_creation_timestamp_column: str
    ) -> None:
        """
        Determines the column in the table that represents the timestamp for record creation. This information is
        utilized to analyze feature job settings, and the identified column is treated as a special column that is
        not meant for feature engineering. By default, any view created from the table will exclude this column.

        Parameters
        ----------
        record_creation_timestamp_column: str
            The column for the timestamp when a record was created.

        Examples
        --------
        Update record creation timestamp column

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table.update_record_creation_timestamp_column("record_available_at")
        """
        self.update(
            update_payload={"record_creation_timestamp_column": record_creation_timestamp_column},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    @typechecked
    def update_status(self, status: Union[TableStatus, str]) -> None:
        """
        Update table status

        Parameters
        ----------
        status: Union[TableStatus, str]
            Table status

        Examples
        --------
        Update table status

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table.update_status(fb.TableStatus.PUBLIC_DRAFT)
        """
        status_value = TableStatus(status).value
        self.update(update_payload={"status": status_value}, allow_update_local=False)

    @staticmethod
    def _validate_view_mode_params(
        view_mode: ViewMode,
        drop_column_names: Optional[List[str]],
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]],
        event_drop_column_names: Optional[List[str]] = None,
        event_column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
        event_join_column_names: Optional[List[str]] = None,
    ) -> None:
        """
        Validate parameters passed from_*_data method

        Parameters
        ----------
        view_mode: ViewMode
            ViewMode
        drop_column_names: Optional[List[str]]
            List of column names to drop
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of column cleaning operations
        event_drop_column_names: Optional[List[str]]
            List of event column names to drop
        event_column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            List of event column cleaning operations
        event_join_column_names: Optional[List[str]]
            List of event join column names

        Raises
        ------
        ValueError
            If any of the parameters are passed in auto mode
        """
        manual_mode_only_params = {
            "drop_column_names": drop_column_names,
            "column_cleaning_operations": column_cleaning_operations,
            "event_drop_column_names": event_drop_column_names,
            "event_column_cleaning_operations": event_column_cleaning_operations,
            "event_join_column_names": event_join_column_names,
        }
        non_empty_params = [name for name, value in manual_mode_only_params.items() if value]
        if view_mode == ViewMode.AUTO and any(non_empty_params):
            params = ", ".join(non_empty_params)
            is_or_are = "is" if len(non_empty_params) == 1 else "are"
            raise ValueError(f"{params} {is_or_are} only supported in manual mode")

    @staticmethod
    def _prepare_table_data_and_column_cleaning_operations(
        table_data: TableDataT,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]],
        view_mode: ViewMode,
    ) -> Tuple[TableDataT, List[ColumnCleaningOperation]]:
        column_cleaning_operations = column_cleaning_operations or []
        if view_mode == ViewMode.MANUAL:
            table_data = table_data.clone(column_cleaning_operations=column_cleaning_operations)
        else:
            column_cleaning_operations = [
                ColumnCleaningOperation(
                    column_name=col.name,
                    cleaning_operations=col.critical_data_info.cleaning_operations,
                )
                for col in table_data.columns_info
                if col.critical_data_info and col.critical_data_info.cleaning_operations
            ]
        return table_data, column_cleaning_operations

    @typechecked
    def update_column_entity(self, column_name: str, entity_name: Optional[str]) -> None:
        """
        Update column entity

        Parameters
        ----------
        column_name: str
            Column name
        entity_name: Optional[str]
            Entity name
        """
        if entity_name is None:
            entity_id = None
        else:
            entity = Entity.get(entity_name)
            entity_id = str(entity.id)

        self.update(
            update_payload={
                "column_name": column_name,
                "entity_id": entity_id,
            },
            allow_update_local=False,
            url=f"{self._route}/{self.id}/column_entity",
            skip_update_schema_check=True,
        )

    @typechecked
    def update_column_critical_data_info(
        self, column_name: str, critical_data_info: CriticalDataInfo
    ) -> None:
        """
        Update column critical data info

        Parameters
        ----------
        column_name: str
            Column name
        critical_data_info: CriticalDataInfo
            Column critical data info
        """
        self.update(
            update_payload={
                "column_name": column_name,
                "critical_data_info": critical_data_info.model_dump(),
            },
            allow_update_local=False,
            url=f"{self._route}/{self.id}/column_critical_data_info",
            skip_update_schema_check=True,
        )

    @typechecked
    def update_column_description(self, column_name: str, description: Optional[str]) -> None:
        """
        Update column description

        Parameters
        ----------
        column_name: str
            Column name
        description: Optional[str]
            Column description
        """
        self.update(
            update_payload={
                "column_name": column_name,
                "description": description,
            },
            allow_update_local=False,
            url=f"{self._route}/{self.id}/column_description",
            skip_update_schema_check=True,
        )

    def delete(self) -> None:
        """
        Delete the table from the persistent data store. The table can only be deleted if

        - the table is not referenced by any other table (item table referencing event table)
        - the table is not referenced by any feature
        - the table is not referenced by any feature job setting analysis
        - the table is not used as primary table in any entity

        Examples
        --------
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table.delete()  # doctest: +SKIP
        """
        self._delete()

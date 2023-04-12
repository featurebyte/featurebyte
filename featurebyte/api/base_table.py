"""
DataColumn class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, Tuple, Type, TypeVar, cast

from http import HTTPStatus

from bson.objectid import ObjectId
from pandas import DataFrame
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping, SavableApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.source_table import AbstractTableData, SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.core.mixin import GetAttrMixin, ParentMixin, SampleMixin
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


class TableColumn(FeatureByteBaseModel, ParentMixin, SampleMixin):
    """
    TableColumn class that is used to set metadata such as Entity column. It holds a reference to its
    parent, which is a table object (e.g. EventTable)
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.TableColumn")

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
    def feature_store(self) -> FeatureStoreModel:
        """
        Feature store used by parent frame

        Returns
        -------
        FeatureStoreModel
        """
        return cast(FeatureStoreModel, self.parent.feature_store)

    def _prepare_columns_info(self, column_info: ColumnInfo) -> List[ColumnInfo]:
        """
        Prepare columns info attribute of the update payload. The output of this function is used as part of the
        table update route payload.

        Parameters
        ----------
        column_info: ColumnInfo
            Column info is used to replace the item of `columns_info` of this object.

        Returns
        -------
        List[ColumnInfo]
        """
        columns_info = []
        for col in self.parent.columns_info:
            if col.name == column_info.name:
                columns_info.append(column_info)
            else:
                columns_info.append(col)
        return columns_info

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: Optional[str]
            Associate column name to the entity, remove association if entity name is None

        Examples
        --------
        Set column "GroceryInvoiceGuid" of the target event table as entity "groceryinvoice"

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["GroceryInvoiceGuid"].as_entity("groceryinvoice")
        """
        if entity_name is None:
            entity_id = None
        else:
            entity = Entity.get(entity_name)
            entity_id = entity.id

        column_info = ColumnInfo(**{**self.info.dict(), "entity_id": entity_id})
        self.parent.update(
            update_payload={"columns_info": self._prepare_columns_info(column_info)},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    @typechecked
    def update_critical_data_info(self, cleaning_operations: List[CleaningOperation]) -> None:
        """
        Associates metadata with the column such as default cleaning operations.

        The cleaning operations dictate how data in the column is cleaned before any feature engineering is performed.
        The operations can be set as a default in the column's metadata, or they can be established when creating a
        view in a manual mode.

        The cleaning operations are designed to handle several scenarios, including missing values, disguised values,
        values that are not in an anticipated list, and numeric values and dates that are out of boundaries. The
        cleaning operations for a specific column may consist of a sequence of operations that must be executed in a
        specific order. Values that have been imputed during previous operations should not be identified as values to
        be cleaned in subsequent operations.

        If changes occur in the data quality of the source table, new versions of the feature can be created with new
        cleaning operations that address the new quality issues.

        Parameters
        ----------
        cleaning_operations: List[CleaningOperation]
            List of cleaning operations to be applied on the column.

        Examples
        --------
        Add missing value imputation & negative value imputation operations to a table column.

        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(
        ...    cleaning_operations=[
        ...        fb.MissingValueImputation(imputed_value=0),
        ...        fb.ValueBeyondEndpointImputation(
        ...            type="less_than", end_point=0, imputed_value=0
        ...        ),
        ...    ]
        ... )

        Show column cleaning operations of the event table.

        >>> event_table.column_cleaning_operations
        [ColumnCleaningOperation(column_name='Amount', cleaning_operations=[MissingValueImputation(imputed_value=0,
        type=missing), ValueBeyondEndpointImputation(imputed_value=0, type=less_than, end_point=0)])]

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
        column_info = ColumnInfo(**{**self.info.dict(), "critical_data_info": critical_data_info})
        self.parent.update(
            update_payload={"columns_info": self._prepare_columns_info(column_info)},
            allow_update_local=True,
            add_internal_prefix=True,
        )

    def extract_pruned_graph_and_node(self) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Returns
        -------
        tuple[QueryGraphModel, Node]
            QueryGraph & mapped Node object (within the pruned graph)
        """
        target_node = self.parent.frame.node
        pruned_graph, node_name_map = self.parent.frame.graph.prune(
            target_node=target_node, aggressive=True
        )
        mapped_node = pruned_graph.get_node_by_name(node_name_map[target_node.name])
        project_node = pruned_graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [self.info.name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[mapped_node],
        )
        return pruned_graph, project_node

    @typechecked
    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        return GraphInterpreter(
            pruned_graph, source_type=self.feature_store.type
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]


class TableListMixin(ApiObject):
    """
    Mixin to implement source table list function
    """

    _route = "/table"
    _list_schema = ProxyTableModel
    _list_fields = ["name", "type", "status", "entities", "created_at"]
    _list_foreign_keys = [
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


class TableApiObject(AbstractTableData, TableListMixin, SavableApiObject, GetAttrMixin):
    """
    Base class for all Table objects
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Table")

    _create_schema_class: ClassVar[Optional[Type[FeatureByteBaseModel]]] = None

    # pydantic instance variable
    type: Literal[
        TableDataType.SOURCE_TABLE,
        TableDataType.EVENT_TABLE,
        TableDataType.ITEM_TABLE,
        TableDataType.DIMENSION_TABLE,
        TableDataType.SCD_TABLE,
    ] = Field(
        description="Table type. Either source_table, event_table, item_table, dimension_table or scd_table."
    )
    saved: bool = Field(
        default=False,
        allow_mutation=False,
        exclude=True,
        description="Flag to indicate whether the Table object is saved in the FeatureByte catalog.",
    )

    # pydantic instance variable (internal use)
    internal_record_creation_timestamp_column: Optional[str] = Field(
        alias="record_creation_timestamp_column"
    )

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs in the table model list.

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.entity_ids  # pylint: disable=no-member

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
            return self._table_data_class(**self.cached_model.json_dict())
        except RecordRetrievalException:
            return self._table_data_class(**self.json_dict())

    @property
    def columns_info(self) -> List[ColumnInfo]:
        """
        List of column information of the table. Each column contains column name, column type, entity ID
        associated with the column, semantic ID associated with the column, and the critical data information
        associated with the column.

        Returns
        -------
        List[ColumnInfo]
            List of column information.

        See Also
        --------
        - [Table.column_cleaning_operations](/reference/featurebyte.api.base_table.TableApiObject.column_cleaning_operations)
        - [TableColumn.update_critical_data_info](/reference/featurebyte.api.base_table.TableColumn.update_critical_data_info)
        """
        try:
            return self.cached_model.columns_info  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.internal_columns_info

    @property
    def catalog_id(self) -> ObjectId:
        """
        Catalog ID of the table. A FeatureByte Catalog serves as a centralized repository for storing metadata.

        Returns
        -------
        ObjectId
            Catalog ID of the table.

        See Also
        --------
        - [Catalog](/reference/featurebyte.api.catalog.Catalog)
        """
        return self.cached_model.catalog_id  # pylint: disable=no-member

    @property
    def primary_key_columns(self) -> List[str]:
        """
        List of primary key columns of the table.

        Returns
        -------
        List[str]
            List of primary key columns
        """
        return self.cached_model.primary_key_columns  # pylint: disable=no-member

    @property
    def status(self) -> TableStatus:
        """
        Table status. Either "DRAFT", "PUBLISHED", or "DEPRECATED".

        Returns
        -------
        TableStatus
            Table status
        """
        try:
            return self.cached_model.status  # pylint: disable=no-member
        except RecordRetrievalException:
            return TableStatus.PUBLIC_DRAFT

    @property
    def record_creation_timestamp_column(self) -> Optional[str]:
        """
        Record creation timestamp column name of this table.

        Returns
        -------
        Optional[str]
            Record creation timestamp column name
        """
        try:
            return self.cached_model.record_creation_timestamp_column  # pylint: disable=no-member
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
        Show the list of column cleaning operations of an event table

        >>> event_table = catalog.get_table("GROCERYINVOICE")
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
        data = self._create_schema_class(**self.json_dict())  # pylint: disable=not-callable
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
        data = cls._create_schema_class(  # pylint: disable=not-callable
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
                    **data.json_dict(),
                    feature_store=source_table.feature_store,
                    _validate_schema=True,
                )
                table.save()
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
            Record creation timestamp column used to perform feature job setting analysis

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

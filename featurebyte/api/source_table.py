"""
SourceTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Type, TypeVar, Union

from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ConstructGraphMixin, FeatureStoreModel
from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.table import AllTableDataT, SouceTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.observation_table import ObservationTableCreate

if TYPE_CHECKING:
    from featurebyte.api.dimension_table import DimensionTable
    from featurebyte.api.event_table import EventTable
    from featurebyte.api.item_table import ItemTable
    from featurebyte.api.observation_table import ObservationTable
    from featurebyte.api.scd_table import SCDTable
else:
    DimensionTable = TypeVar("DimensionTable")
    EventTable = TypeVar("EventTable")
    ItemTable = TypeVar("ItemTable")
    SCDTable = TypeVar("SCDTable")


class TableDataFrame(BaseFrame):
    """
    TableDataFrame class is a frame encapsulation of the table objects (like event table, item table).
    This class is used to construct the query graph for previewing/sampling underlying table stored at the
    data warehouse. The constructed query graph is stored locally (not loaded into the global query graph).
    """

    table_data: BaseTableData

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        node = self.node
        if kwargs.get("after_cleaning"):
            assert isinstance(node, InputNode)
            graph_node = self.table_data.construct_cleaning_recipe_node(
                input_node=node, skip_column_names=[]
            )
            if graph_node:
                node = self.graph.add_node(node=graph_node, input_nodes=[self.node])

        pruned_graph, node_name_map = self.graph.prune(target_node=node, aggressive=True)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[node.name])
        return pruned_graph, mapped_node


class AbstractTableData(ConstructGraphMixin, FeatureByteBaseModel, ABC):
    """
    AbstractTableDataFrame class represents the table.
    """

    # class variables
    _table_data_class: ClassVar[Type[AllTableDataT]]

    # pydantic instance variable (public)
    tabular_source: TabularSource = Field(allow_mutation=False)
    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)

    # pydantic instance variable (internal use)
    internal_columns_info: List[ColumnInfo] = Field(alias="columns_info")

    def __init__(self, **kwargs: Any):
        # Construct feature_store, input node & set the graph related parameters based on the given input dictionary
        values = kwargs
        tabular_source = dict(values["tabular_source"])
        table_details = tabular_source["table_details"]
        if "feature_store" not in values:
            # attempt to set feature_store object if it does not exist in the input
            from featurebyte.api.feature_store import (  # pylint: disable=import-outside-toplevel,cyclic-import
                FeatureStore,
            )

            values["feature_store"] = FeatureStore.get_by_id(id=tabular_source["feature_store_id"])

        feature_store = values["feature_store"]
        if isinstance(table_details, dict):
            table_details = TableDetails(**table_details)

        to_validate_schema = values.get("_validate_schema") or "columns_info" not in values
        if to_validate_schema:
            client = Configurations().get_client()
            response = client.post(
                url=(
                    f"/feature_store/column?"
                    f"database_name={table_details.database_name}&"
                    f"schema_name={table_details.schema_name}&"
                    f"table_name={table_details.table_name}"
                ),
                json=feature_store.json_dict(),
            )
            if response.status_code == HTTPStatus.OK:
                column_specs = response.json()
                recent_schema = {
                    column_spec["name"]: DBVarType(column_spec["dtype"])
                    for column_spec in column_specs
                }
            else:
                raise RecordRetrievalException(response)

            if "columns_info" in values:
                columns_info = [ColumnInfo(**dict(col)) for col in values["columns_info"]]
                schema = {col.name: col.dtype for col in columns_info}
                if not recent_schema.items() >= schema.items():
                    logger.warning("Table schema has been changed.")
            else:
                columns_info = [
                    ColumnInfo(name=name, dtype=var_type)
                    for name, var_type in recent_schema.items()
                ]
                values["columns_info"] = columns_info

        # call pydantic constructor to validate input parameters
        super().__init__(**values)

    @property
    def table_data(self) -> BaseTableData:
        """
        Table data object of this table. This object contains information used to construct the SQL query.

        Returns
        -------
        BaseTableData
            Table data object used for SQL query construction.
        """
        return self._table_data_class(**self.json_dict())

    @property
    def frame(self) -> TableDataFrame:
        """
        Frame object of this table object. The frame is constructed from a local query graph.

        Returns
        -------
        TableDataFrame
        """
        # Note that the constructed query graph WILL NOT BE INSERTED into the global query graph.
        # Only when the view is constructed, the local query graph (constructed by table object) is then loaded
        # into the global query graph.
        graph, node = self.construct_graph_and_node(
            feature_store_details=self.feature_store.get_feature_store_details(),
            table_data_dict=self.table_data.dict(by_alias=True),
        )
        return TableDataFrame(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            table_data=self.table_data,
            columns_info=self.columns_info,
            graph=graph,
            node_name=node.name,
        )

    @property
    def column_var_type_map(self) -> dict[str, DBVarType]:
        """
        Column name to DB var type mapping.

        Returns
        -------
        dict[str, DBVarType]
        """
        return {col.name: col.dtype for col in self.columns_info}

    @property
    def dtypes(self) -> pd.Series:
        """
        Retrieve column table type info.

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

    @property
    def columns(self) -> list[str]:
        """
        List of column names of this table.

        Returns
        -------
        list[str]
            List of column name strings.
        """
        return list(self.column_var_type_map)

    @typechecked
    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformation output.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit)

    @typechecked
    def preview_clean_data_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the table after applying list of cleaning operations.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit, after_cleaning=True)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve a preview of the table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        Preview 3 rows of the table.
        >>> catalog.get_table("GROCERYPRODUCT").preview(3)
                             GroceryProductGuid ProductGroup
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f      Glaçons
        1  116c9284-2c41-446e-8eee-33901e0acdef      Glaçons
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375      Glaçons

        See Also
        --------
        - [Table.sample](/reference/featurebyte.api.base_table.TableApiObject.sample/):
          Retrieve a sample of a table.
        - [Table.describe](/reference/featurebyte.api.base_table.TableApiObject.describe/):
          Retrieve a summary of a table.
        """
        return self.frame.preview(limit=limit, after_cleaning=after_cleaning, **kwargs)  # type: ignore[misc]

    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a random sample of the table.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to apply cleaning operations.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.

        Examples
        --------
        Sample 3 rows from the table.
        >>> catalog.get_table("GROCERYPRODUCT").sample(3)
                             GroceryProductGuid ProductGroup
        0  e890c5cb-689b-4caf-8e49-6b97bb9420c0       Épices
        1  5720e4df-2996-4443-a1bc-3d896bf98140         Chat
        2  96fc4d80-8cb0-4f1b-af01-e71ad7e7104a        Pains

        See Also
        --------
        - [Table.preview](/reference/featurebyte.api.base_table.TableApiObject.preview/):
          Retrieve a preview of a table.
        - [Table.describe](/reference/featurebyte.api.base_table.TableApiObject.describe/):
          Retrieve a summary of a table.
        """
        return self.frame.sample(  # type: ignore[misc]
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a summary of the contents in the table.
        This includes columns names, column types, missing and unique counts, and other statistics.

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
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Summary of the table.

        Examples
        --------
        Get a summary of a view.
        >>> catalog.get_table("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                         1                1319

        See Also
        --------
        - [Table.preview](/reference/featurebyte.api.base_table.TableApiObject.preview/):
          Retrieve a preview of a table.
        - [Table.sample](/reference/featurebyte.api.base_table.TableApiObject.sample/):
          Retrieve a sample of a table.
        """
        return self.frame.describe(  # type: ignore[misc]
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )

    @property
    @abstractmethod
    def columns_info(self) -> List[ColumnInfo]:
        """
        List of column information of the dataset. Each column contains column name, column type, entity ID
        associated with the column, semantic ID associated with the column.

        Returns
        -------
        List[ColumnInfo]
        """


class SourceTable(AbstractTableData):
    """
    SourceTable class to preview table.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SourceTable")

    _table_data_class: ClassVar[Type[AllTableDataT]] = SouceTableData

    @property
    def columns_info(self) -> List[ColumnInfo]:
        return self.internal_columns_info

    @typechecked
    def create_event_table(
        self,
        name: str,
        event_timestamp_column: str,
        event_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> EventTable:
        """
        Create event table from this source table.

        Parameters
        ----------
        name: str
            Event table name.
        event_id_column: str
            Event ID column from the given source table.
        event_timestamp_column: str
            Event timestamp column from the given source table.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            event table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        EventTable
            EventTable created from the source table.

        Examples
        --------
        Create an event table from a source table.

        >>> grocery_invoice_table = event_source_table.create_event_table(  # doctest: +SKIP
        ...   name="GROCERYINVOICE",
        ...   event_id_column="GroceryInvoiceGuid",
        ...   event_timestamp_column="Timestamp",
        ...   record_creation_timestamp_column="record_available_at",
        ... )
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.event_table import EventTable

        return EventTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
            _id=_id,
        )

    @typechecked
    def create_item_table(
        self,
        name: str,
        event_id_column: str,
        item_id_column: str,
        event_table_name: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> ItemTable:
        """
        Create item table from this source table.

        Parameters
        ----------
        name: str
            Item table name.
        event_id_column: str
            Event ID column from the given source table.
        item_id_column: str
            Item ID column from the given source table.
        event_table_name: str
            Name of the EventTable associated with this ItemTable.
        record_creation_timestamp_column: Optional[str]
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            item table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        ItemTable
            ItemTable created from the source table.

        Examples
        --------
        Create an item table from a source table.

        >>> grocery_items_table = item_source_table.create_item_table(  # doctest: +SKIP
        ...   name="INVOICEITEMS",
        ...   event_id_column="GroceryInvoiceGuid",
        ...   item_id_column="GroceryInvoiceItemGuid",
        ...   event_table_name="GROCERYINVOICE",
        ... )
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.event_table import EventTable
        from featurebyte.api.item_table import ItemTable

        event_table = EventTable.get(event_table_name)
        return ItemTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_table_id=event_table.id,
            _id=_id,
        )

    @typechecked
    def create_dimension_table(
        self,
        name: str,
        dimension_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> DimensionTable:
        """
        Create dimension table from this source table.

        Parameters
        ----------
        name: str
            Dimension table name.
        dimension_id_column: str
            Dimension table ID column from the given tabular source.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given tabular source.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            dimension table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        DimensionTable
            DimensionTable created from the source table.

        Examples
        --------
        Create a dimension table from a source table.

        >>> grocery_product_table = dimension_source_table.create_dimension_table(  # doctest: +SKIP
        ...   name="GROCERYPRODUCT",  # doctest: +SKIP
        ...   dimension_id_column="GroceryProductGuid",
        ... )
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.dimension_table import DimensionTable

        return DimensionTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            dimension_id_column=dimension_id_column,
            _id=_id,
        )

    @typechecked
    def create_scd_table(
        self,
        name: str,
        natural_key_column: str,
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SCDTable:
        """
        Create SCD table from this source table.

        Parameters
        ----------
        name: str
            SCDTable name.
        natural_key_column: str
            Natural key column from the given source table.
        effective_timestamp_column: str
            Effective timestamp column from the given source table.
        end_timestamp_column: Optional[str]
            End timestamp column from the given source table.
        surrogate_key_column: Optional[str]
            Surrogate key column from the given source table. A surrogate key is a unique identifier assigned to
            each record, and is used to provide a stable identifier for data even as it changes over time.
        current_flag_column: Optional[str]
            Column to indicate whether the keys are for the current time in point.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            SCD table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        SCDTable
            SCDTable created from the source table.

        Examples
        --------
        Create a SCD table from a source table.

        >>> grocery_customer_table = scd_source_table.create_scd_table(  # doctest: +SKIP
        ...   name="GROCERYCUSTOMER",
        ...   surrogate_key_column="RowID",
        ...   natural_key_column="GroceryCustomerGuid",
        ...   effective_timestamp_column="ValidFrom",
        ...   current_flag_column="CurrentRecord",
        ...   record_creation_timestamp_column="record_available_at",
        ... )
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.scd_table import SCDTable

        return SCDTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            _id=_id,
            natural_key_column=natural_key_column,
            surrogate_key_column=surrogate_key_column,
            effective_timestamp_column=effective_timestamp_column,
            end_timestamp_column=end_timestamp_column,
            current_flag_column=current_flag_column,
        )

    @typechecked
    def get_or_create_event_table(
        self,
        name: str,
        event_timestamp_column: str,
        event_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> EventTable:
        """
        Get or create event table from this source table. Internally, this method calls `EventTable.get` by name,
        if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            Event table name.
        event_id_column: str
            Event ID column from the given source table.
        event_timestamp_column: str
            Event timestamp column from the given source table.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            event table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        EventTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.event_table import EventTable

        return EventTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
            _id=_id,
        )

    @typechecked
    def get_or_create_item_table(
        self,
        name: str,
        event_id_column: str,
        item_id_column: str,
        event_table_name: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> ItemTable:
        """
        Get or create item from this source table. Internally, this method calls `ItemTable.get` by name,
        if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            Item table name.
        event_id_column: str
            Event ID column from the given source table.
        item_id_column: str
            Item ID column from the given source table.
        event_table_name: str
            Name of the EventTable associated with this ItemTable.
        record_creation_timestamp_column: Optional[str]
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            item table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        ItemTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.event_table import EventTable
        from featurebyte.api.item_table import ItemTable

        event_table = EventTable.get(event_table_name)
        return ItemTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_table_id=event_table.id,
            _id=_id,
        )

    @typechecked
    def get_or_create_dimension_table(
        self,
        name: str,
        dimension_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> DimensionTable:
        """
        Get or create dimension table from this source table. Internally, this method calls `DimensionTable.get`
        by name, if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            Dimension table name.
        dimension_id_column: str
            Dimension table ID column from the given tabular source.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given tabular source.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            dimension table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        DimensionTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.dimension_table import DimensionTable

        return DimensionTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            dimension_id_column=dimension_id_column,
            _id=_id,
        )

    @typechecked
    def get_or_create_scd_table(
        self,
        name: str,
        natural_key_column: str,
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SCDTable:
        """
        Get or create SCD table from this source table. Internally, this method calls `SCDTable.get` by name,
        if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            SCDTable name.
        natural_key_column: str
            Natural key column from the given source table.
        effective_timestamp_column: str
            Effective timestamp column from the given source table.
        end_timestamp_column: Optional[str]
            End timestamp column from the given source table.
        surrogate_key_column: Optional[str]
            Surrogate key column from the given source table. A surrogate key is a unique identifier assigned to
            each record, and is used to provide a stable identifier for data even as it changes over time.
        current_flag_column: Optional[str]
            Column to indicate whether the keys are for the current time in point.
        record_creation_timestamp_column: str
            Record creation timestamp column from the given source table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            SCD table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        SCDTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.scd_table import SCDTable

        return SCDTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            _id=_id,
            natural_key_column=natural_key_column,
            surrogate_key_column=surrogate_key_column,
            effective_timestamp_column=effective_timestamp_column,
            end_timestamp_column=end_timestamp_column,
            current_flag_column=current_flag_column,
        )

    def create_observation_table(
        self,
        name: str,
        sample_rows: Optional[int] = None,
    ) -> ObservationTable:
        """
        Create an observation table from this source table.

        Parameters
        ----------
        name: str
            Observation table name.
        sample_rows: Optional[int]
            Optionally sample the source table to this number of rows before creating the
            observation table.

        Returns
        -------
        ObservationTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.observation_table import ObservationTable

        payload = ObservationTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            observation_input=SourceTableObservationInput(source=self.tabular_source),
            sample_rows=sample_rows,
        )
        observation_table_doc = ObservationTable.post_async_task(
            route="/observation_table", payload=payload.json_dict()
        )
        return ObservationTable.get_by_id(observation_table_doc["_id"])

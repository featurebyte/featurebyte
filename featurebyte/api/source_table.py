"""
SourceTable class
"""
# pylint: disable=too-many-lines
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple, Type, TypeVar, Union, cast

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
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ConstructGraphMixin, FeatureStoreModel
from featurebyte.models.request_input import SourceTableRequestInput
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.table import AllTableDataT, SourceTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.observation_table import ObservationTableCreate

if TYPE_CHECKING:
    from featurebyte.api.batch_request_table import BatchRequestTable
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


logger = get_logger(__name__)


class TableDataFrame(BaseFrame):
    """
    TableDataFrame class is a frame encapsulation of the table objects (like event table, item table).
    This class is used to construct the query graph for previewing/sampling underlying table stored at the
    data warehouse. The constructed query graph is stored locally (not loaded into the global query graph).
    """

    table_data: BaseTableData
    int_timestamp_column: Optional[str] = Field(alias="timestamp_column")

    @property
    def timestamp_column(self) -> Optional[str]:
        return self.int_timestamp_column

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
            timestamp_column=self.timestamp_column,
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
        Returns a Series with the data type of each column in the table.

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

    @property
    def columns(self) -> list[str]:
        """
        Returns the list of column names of this table.

        Returns
        -------
        list[str]
            List of column name strings.
        """
        return list(self.column_var_type_map)

    @typechecked
    def preview_sql(self, limit: int = 10, after_cleaning: bool = False) -> str:
        """
        Returns an SQL query for previewing the table raw data.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit, after_cleaning=after_cleaning)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the table.

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
        return self.frame.preview(limit=limit, after_cleaning=after_cleaning)

    @typechecked
    def shape(self, after_cleaning: bool = False) -> Tuple[int, int]:
        """
        Return the shape of the view / column.

        Parameters
        ----------
        after_cleaning: bool
            Whether to get the shape of the table after cleaning

        Returns
        -------
        Tuple[int, int]

        Examples
        --------
        Get the shape of a table.
        >>> catalog.get_table("INVOICEITEMS").shape()
        (300450, 8)
        """
        return cast(Tuple[int, int], self.frame.shape(after_cleaning=after_cleaning))

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
        Returns a DataFrame that contains a random selection of rows of the table based on a specified time range,
        size, and seed for sampling control. By default, the materialization process occurs before any cleaning
        operations that were defined at the column level.

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
        return self.frame.sample(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )

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
        Returns descriptive statistics of the table columns.

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
        Get a summary of a view.
        >>> catalog.get_table("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                       1.0              1319.0

        See Also
        --------
        - [Table.preview](/reference/featurebyte.api.base_table.TableApiObject.preview/):
          Retrieve a preview of a table.
        - [Table.sample](/reference/featurebyte.api.base_table.TableApiObject.sample/):
          Retrieve a sample of a table.
        """
        return self.frame.describe(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )

    @property
    @abstractmethod
    def timestamp_column(self) -> Optional[str]:
        """
        Name of the timestamp column.

        Returns
        -------
        Optional[str]
        """

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

    _table_data_class: ClassVar[Type[AllTableDataT]] = SourceTableData

    @property
    def timestamp_column(self) -> Optional[str]:
        return None

    @property
    def columns_info(self) -> List[ColumnInfo]:
        return self.internal_columns_info

    @typechecked
    def create_event_table(
        self,
        name: str,
        event_timestamp_column: str,
        event_id_column: str,
        event_timestamp_timezone_offset: Optional[str] = None,
        event_timestamp_timezone_offset_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> EventTable:
        """
        Creates and adds to the catalog an EventTable object from a source table where each row indicates a specific
        business event measured at a particular moment.

        To create an EventTable, you need to identify the columns representing the event key and timestamp.

        Additionally, the column that represents the record creation timestamp may be identified to enable an automatic
        analysis of data availability and freshness of the source table. This analysis can assist in selecting the
        default scheduling of the computation of features associated with the Event table (default FeatureJob setting).

        After creation, the table can optionally incorporate additional metadata at the column level to further aid
        feature engineering. This can include identifying columns that identify or reference entities, providing
        information about the semantics of the table columns, specifying default cleaning operations, or furnishing
        descriptions of its columns.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        event_id_column: str
            The column that represents the unique identfier for each event.
        event_timestamp_column: str
            The column that contains the timestamp of the associated event.
        event_timestamp_timezone_offset: Optional[str]
            Timezone offset for the event timestamp column. Supported format is "(+|-)HH:mm".
            Specify this if the timezone offset is a fixed value. Examples: "+08:00" or "-05:00".
        event_timestamp_timezone_offset_column: Optional[str]
            Timezone offset column for the event timestamp column. The column is expected to have
            string type, and each value in the column is expected to have the format "(+|-)HH:mm".
            Specify this if the timezone offset is different for different rows in the event table.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
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

        >>> # Register GroceryInvoice as an event data
        >>> source_table = ds.get_table(  # doctest: +SKIP
        ...   database_name="spark_catalog",
        ...   schema_name="GROCERY",
        ...   table_name="GROCERYINVOICE"
        ... )
        >>> invoice_table = source_table.create_event_table(  # doctest: +SKIP
        ...   name="GROCERYINVOICE",
        ...   event_id_column="GroceryInvoiceGuid",
        ...   event_timestamp_column="Timestamp",
        ...   record_creation_timestamp_column="record_available_at"
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
            event_timestamp_timezone_offset=event_timestamp_timezone_offset,
            event_timestamp_timezone_offset_column=event_timestamp_timezone_offset_column,
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
        Creates and adds to the catalog an ItemTable object from a source table containing in-depth details about
        a business event.

        To create an Item Table, you need to identify the columns representing the item key and the event key and
        determine which Event table is associated with the Item table.

        After creation, the table can optionally incorporate additional metadata at the column level to further aid
        feature engineering. This can include identifying columns that identify or reference entities, providing
        information about the semantics of the table columns, specifying default cleaning operations, or furnishing
        descriptions of its columns.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        event_id_column: str
            The column that represents the unique identifier for the associated event. This column will be used to join
            the item table with the event table.
        item_id_column: str
            The column that represents the unique identifier for each item.
        event_table_name: str
            The name of the event table that the item table will be associated with. This is used to ensure that the
            item table is properly linked to the correct event table.
        record_creation_timestamp_column: Optional[str]
            The optional column for the timestamp when a record was created.
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

        >>> # Register invoice items as an item table
        >>> source_table = ds.get_table(  # doctest: +SKIP
        ...   database_name="spark_catalog",
        ...   schema_name="GROCERY",
        ...   table_name="INVOICEITEMS"
        ... )
        >>> items_table.create_item_table(  # doctest: +SKIP
        ...   name="INVOICEITEMS",
        ...   event_id_column="GroceryInvoiceGuid",
        ...   item_id_column="GroceryInvoiceItemGuid",
        ...   event_table_name="GROCERYINVOICE"
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
        Creates and adds to the catalog a DimensionTable object from a source table that holds static descriptive
        information.

        To create a Dimension Table, you need to identify the column representing the primary key of the source table
        (dimension_id_column).

        After creation, the table can optionally incorporate additional metadata at the column level to further
        aid feature engineering. This can include identifying columns that identify or reference entities,
        providing information about the semantics of the table columns, specifying default cleaning operations, or
        furnishing descriptions of its columns.

        Note that using a Dimension table requires special attention. If the data in the table changes slowly, it is
        not advisable to use it because these changes can cause significant data leaks during model training and
        adversely affect the inference performance. In such cases, it is recommended to use a Type 2 Slowly Changing
        Dimension table that maintains a history of changes.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        dimension_id_column: str
            The column that serves as the primary key, uniquely identifying each record in the table.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
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

        >>> # Register GroceryProduct as a dimension table
        >>> source_table = ds.get_table(  # doctest: +SKIP
        ...   database_name="spark_catalog",
        ...   schema_name="GROCERY",
        ...   table_name="GROCERYPRODUCT"
        ... )
        >>> product_table = source_table.create_dimension_table(  # doctest: +SKIP
        ...   name="GROCERYPRODUCT,
        ...   dimension_id_column="GroceryProductGuid"
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
        Creates and adds to the catalog a SCDTable object from a source table that contains data that changes slowly
        and unpredictably over time, also known as a Slowly Changing Dimension (SCD) table.

        Please note that there are two main types of SCD Tables:

        - Type 1: Overwrites old data with new data
        - Type 2: Maintains a history of changes by creating a new record for each change.

        eatureByte only supports the use of Type 2 SCD Tables since Type 1 SCD Tables may cause data leaks during
        model training and poor performance during inference.

        To create an SCD Table, you need to identify columns for the natural key, effective timestamp, optionally
        surrogate key, end timestamp, and active flag.

        An SCD table of Type 2 utilizes the natural key to distinguish each active row and facilitate tracking of
        changes over time. The SCD table employs the effective and end (or expiration) timestamp columns to determine
        the active status of a row. In certain instances, an active flag column may replace the expiration timestamp
        column to indicate if a row is currently active.

        After creation, the table can optionally incorporate additional metadata at the column level to further aid
        feature engineering. This can include identifying columns that identify or reference entities, providing
        information about the semantics of the table columns, specifying default cleaning operations, or furnishing
        descriptions of its columns.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        natural_key_column: str
            The column that uniquely identifies active records at a given point-in-time.
        effective_timestamp_column: str
            The column that represents when the record becomes effective (i.e., active).
        end_timestamp_column: Optional[str]
            The optional column for the end or expiration timestamp, indicating when a record is no longer active.
        surrogate_key_column: Optional[str]
            The optional column for a surrogate key that uniquely identifies each row in the table.
        current_flag_column: Optional[str]
            The optional column that shows if a record is currently active or not.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
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

         >>> # Declare the grocery customer table
         >>> source_table = ds.get_table(  # doctest: +SKIP
         ...   database_name="spark_catalog",
         ...   schema_name="GROCERY",
         ...   table_name="GROCERYCUSTOMER"
         ... )
         >>> customer_table = source_table.create_scd_table(  # doctest: +SKIP
         ...    name="GROCERYCUSTOMER",
         ...    surrogate_key_column='RowID',
         ...    natural_key_column="GroceryCustomerGuid",
         ...    effective_timestamp_column="ValidFrom",
         ...    current_flag_column ="CurrentRecord",
         ...    record_creation_timestamp_column="record_available_at"
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
            The desired name for the new table.
        event_id_column: str
            The column that represents the unique identifier for each event.
        event_timestamp_column: str
            The column that contains the timestamp of the associated event.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
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
            request_input=SourceTableRequestInput(source=self.tabular_source),
            sample_rows=sample_rows,
        )
        observation_table_doc = ObservationTable.post_async_task(
            route="/observation_table", payload=payload.json_dict()
        )
        return ObservationTable.get_by_id(observation_table_doc["_id"])

    def create_batch_request_table(
        self,
        name: str,
    ) -> BatchRequestTable:
        """
        Create a batch request table from this source table.

        Parameters
        ----------
        name: str
            Batch request table name.

        Returns
        -------
        BatchRequestTable
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.batch_request_table import BatchRequestTable

        payload = BatchRequestTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=SourceTableRequestInput(source=self.tabular_source),
        )
        batch_request_table_doc = BatchRequestTable.post_async_task(
            route="/batch_request_table", payload=payload.json_dict()
        )
        return BatchRequestTable.get_by_id(batch_request_table_doc["_id"])

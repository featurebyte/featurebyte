"""
SourceTable class
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple, Type, TypeVar, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.entity import Entity
from featurebyte.api.mixin import SampleMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.utils import dataframe_from_json, validate_datetime_input
from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.core.mixin import perf_logging
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.batch_request_table import BatchRequestInput, SourceTableBatchRequestInput
from featurebyte.models.feature_store import ConstructGraphMixin, FeatureStoreModel
from featurebyte.models.observation_table import SourceTableObservationInput
from featurebyte.models.static_source_table import SourceTableStaticSourceInput
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.table import AllTableDataT, SourceTableData
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.feature_store import FeatureStoreShape
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.static_source_table import StaticSourceTableCreate

if TYPE_CHECKING:
    from featurebyte.api.batch_request_table import BatchRequestTable
    from featurebyte.api.dimension_table import DimensionTable
    from featurebyte.api.event_table import EventTable
    from featurebyte.api.item_table import ItemTable
    from featurebyte.api.observation_table import ObservationTable
    from featurebyte.api.scd_table import SCDTable
    from featurebyte.api.static_source_table import StaticSourceTable
    from featurebyte.api.time_series_table import TimeSeriesTable
else:
    DimensionTable = TypeVar("DimensionTable")
    EventTable = TypeVar("EventTable")
    ItemTable = TypeVar("ItemTable")
    SCDTable = TypeVar("SCDTable")


logger = get_logger(__name__)


class TableDataFrame(BaseFrame, SampleMixin):
    """
    TableDataFrame class is a frame encapsulation of the table objects (like event table, item table).
    This class is used to construct the query graph for previewing/sampling underlying table stored at the
    data warehouse. The constructed query graph is stored locally (not loaded into the global query graph).
    """

    # instance variables
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

        pruned_graph, node_name_map = self.graph.prune(target_node=node)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[node.name])
        return pruned_graph, mapped_node


class AbstractTableData(ConstructGraphMixin, FeatureByteBaseModel, ABC):
    """
    AbstractTableDataFrame class represents the table.
    """

    # class variables
    _table_data_class: ClassVar[Type[AllTableDataT]]

    # pydantic instance variable (public)
    tabular_source: TabularSource = Field(frozen=True)
    feature_store: FeatureStoreModel = Field(frozen=True, exclude=True)

    # pydantic instance variable (internal use)
    internal_columns_info: List[ColumnInfo] = Field(alias="columns_info")

    def __init__(self, **kwargs: Any):
        # construct feature_store based on the given input dictionary
        values = kwargs
        tabular_source = dict(values["tabular_source"])
        if "feature_store" not in values:
            # attempt to set feature_store object if it does not exist in the input
            from featurebyte.api.feature_store import (
                FeatureStore,
            )

            values["feature_store"] = FeatureStore.get_by_id(id=tabular_source["feature_store_id"])

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
        return self._table_data_class(**self.model_dump(by_alias=True))

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
            table_data_dict=self.table_data.model_dump(by_alias=True),
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
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
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


        Sample 3 rows from the table with timestamps.
        >>> event_table = catalog.get_table("GROCERYINVOICE")
        >>> event_table["Amount"].update_critical_data_info(  # doctest: +SKIP
        ...     cleaning_operations=[
        ...         fb.MissingValueImputation(imputed_value=0),
        ...     ]
        ... )
        >>> event_table.sample(  # doctest: +SKIP
        ...     size=3,
        ...     seed=111,
        ...     from_timestamp=datetime(2019, 1, 1),
        ...     to_timestamp=datetime(2023, 12, 31),
        ...     after_cleaning=True,
        ... )

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

        >>> catalog.get_table("GROCERYINVOICE").describe(
        ...     from_timestamp=datetime(2022, 1, 1),
        ...     to_timestamp=datetime(2022, 12, 31),
        ... )
                                    GroceryInvoiceGuid                   GroceryCustomerGuid                      Timestamp            record_available_at     Amount
        dtype                                  VARCHAR                               VARCHAR                      TIMESTAMP                      TIMESTAMP      FLOAT
        unique                                   25422                                   471                          25399                           5908       6734
        %missing                                   0.0                                   0.0                            0.0                            0.0        0.0
        %empty                                       0                                     0                            NaN                            NaN        NaN
        entropy                               6.214608                              5.784261                            NaN                            NaN        NaN
        top       018f0163-249b-4cbc-ab4d-e933ce3786c1  c5820998-e779-4d62-ab8b-79ef0dfd841b                            NaN                            NaN        NaN
        freq                                       1.0                                 692.0                            NaN                            NaN        NaN
        mean                                       NaN                                   NaN                            NaN                            NaN  19.966062
        std                                        NaN                                   NaN                            NaN                            NaN  25.027878
        min                                        NaN                                   NaN  2022-01-01T00:24:14.000000000  2022-01-01T01:01:00.000000000        0.0
        25%                                        NaN                                   NaN                            NaN                            NaN     4.5325
        50%                                        NaN                                   NaN                            NaN                            NaN     10.725
        75%                                        NaN                                   NaN                            NaN                            NaN      24.99
        max                                        NaN                                   NaN  2022-12-30T22:37:57.000000000  2022-12-30T23:01:00.000000000     360.84

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

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.SourceTable")
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
        event_id_column: Optional[str],
        event_timestamp_timezone_offset: Optional[str] = None,
        event_timestamp_timezone_offset_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        event_timestamp_schema: Optional[TimestampSchema] = None,
        datetime_partition_column: Optional[str] = None,
        datetime_partition_schema: Optional[TimestampSchema] = None,
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
        event_timestamp_column: str
            The column that contains the timestamp of the associated event.
        event_id_column: Optional[str]
            The column that represents the unique identfier for each event.
        event_timestamp_timezone_offset: Optional[str]
            Timezone offset for the event timestamp column. Supported format is "(+|-)HH:mm".
            Specify this if the timezone offset is a fixed value. Examples: "+08:00" or "-05:00".
        event_timestamp_timezone_offset_column: Optional[str]
            Timezone offset column for the event timestamp column. The column is expected to have
            string type, and each value in the column is expected to have the format "(+|-)HH:mm".
            Specify this if the timezone offset is different for different rows in the event table.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
        description: Optional[str]
            The optional description for the new table.
        event_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the event timestamp column.
        datetime_partition_column: Optional[str]
            The optional column for the datetime column used for partitioning the event table.
        datetime_partition_schema: Optional[TimestampSchema]
            The optional timestamp schema for the datetime partition column.
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
        >>> source_table = ds.get_source_table(  # doctest: +SKIP
        ...     database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYINVOICE"
        ... )
        >>> invoice_table = source_table.create_event_table(  # doctest: +SKIP
        ...     name="GROCERYINVOICE",
        ...     event_id_column="GroceryInvoiceGuid",
        ...     event_timestamp_column="Timestamp",
        ...     record_creation_timestamp_column="record_available_at",
        ... )
        """

        from featurebyte.api.event_table import EventTable

        return EventTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
            event_timestamp_timezone_offset=event_timestamp_timezone_offset,
            event_timestamp_timezone_offset_column=event_timestamp_timezone_offset_column,
            event_timestamp_schema=event_timestamp_schema,
            datetime_partition_column=datetime_partition_column,
            datetime_partition_schema=datetime_partition_schema,
            description=description,
            _id=_id,
        )

    @typechecked
    def create_item_table(
        self,
        name: str,
        event_id_column: str,
        item_id_column: Optional[str],
        event_table_name: str,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
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
        item_id_column: Optional[str]
            The column that represents the unique identifier for each item.
        event_table_name: str
            The name of the event table that the item table will be associated with. This is used to ensure that the
            item table is properly linked to the correct event table.
        record_creation_timestamp_column: Optional[str]
            The optional column for the timestamp when a record was created.
        description: Optional[str]
            The optional description for the new table.
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
        ...     database_name="spark_catalog", schema_name="GROCERY", table_name="INVOICEITEMS"
        ... )
        >>> source_table.create_item_table(  # doctest: +SKIP
        ...     name="INVOICEITEMS",
        ...     event_id_column="GroceryInvoiceGuid",
        ...     item_id_column="GroceryInvoiceItemGuid",
        ...     event_table_name="GROCERYINVOICE",
        ... )
        """

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
            description=description,
            _id=_id,
        )

    @typechecked
    def create_dimension_table(
        self,
        name: str,
        dimension_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
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
        description: Optional[str]
            The optional description of the new table.
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
        ...     database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYPRODUCT"
        ... )
        >>> product_table = source_table.create_dimension_table(  # doctest: +SKIP
        ...     name="GROCERYPRODUCT", dimension_id_column="GroceryProductGuid"
        ... )
        """

        from featurebyte.api.dimension_table import DimensionTable

        return DimensionTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            dimension_id_column=dimension_id_column,
            description=description,
            _id=_id,
        )

    @typechecked
    def create_scd_table(
        self,
        name: str,
        natural_key_column: Optional[str],
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        effective_timestamp_schema: Optional[TimestampSchema] = None,
        end_timestamp_schema: Optional[TimestampSchema] = None,
        _id: Optional[ObjectId] = None,
    ) -> SCDTable:
        """
        Creates and adds to the catalog a SCDTable object from a source table that contains data that changes slowly
        and unpredictably over time, also known as a Slowly Changing Dimension (SCD) table.

        Please note that there are two main types of SCD Tables:

        - Type 1: Overwrites old data with new data
        - Type 2: Maintains a history of changes by creating a new record for each change.

        FeatureByte only supports the use of Type 2 SCD Tables since Type 1 SCD Tables may cause data leaks during
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
        natural_key_column: Optional[str]
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
        description: Optional[str]
            The optional description of the new table.
        effective_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the effective timestamp column.
        end_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the end timestamp column.
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
        ...     database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYCUSTOMER"
        ... )
        >>> customer_table = source_table.create_scd_table(  # doctest: +SKIP
        ...     name="GROCERYCUSTOMER",
        ...     surrogate_key_column="RowID",
        ...     natural_key_column="GroceryCustomerGuid",
        ...     effective_timestamp_column="ValidFrom",
        ...     current_flag_column="CurrentRecord",
        ...     record_creation_timestamp_column="record_available_at",
        ... )

        See Also
        --------
        - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
            Schema for a timestamp column that can include timezone information.
        """

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
            effective_timestamp_schema=effective_timestamp_schema,
            end_timestamp_schema=end_timestamp_schema,
            description=description,
        )

    @typechecked
    def create_time_series_table(
        self,
        name: str,
        reference_datetime_column: str,
        reference_datetime_schema: TimestampSchema,
        time_interval: TimeInterval,
        series_id_column: Optional[str],
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        datetime_partition_column: Optional[str] = None,
        datetime_partition_schema: Optional[TimestampSchema] = None,
        _id: Optional[ObjectId] = None,
    ) -> TimeSeriesTable:
        """
        Creates and adds to the catalog an TimeSeriesTable object from a source table where each row indicates a specific
        business event measured at a particular moment.

        To create an TimeSeriesTable, you need to identify the columns representing the series key and reference datetime..

        After creation, the table can optionally incorporate additional metadata at the column level to further aid
        feature engineering. This can include identifying columns that identify or reference entities, providing
        information about the semantics of the table columns, specifying default cleaning operations, or furnishing
        descriptions of its columns.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        reference_datetime_column: str
            The column that contains the reference datetime of the associated time series.
        reference_datetime_schema: TimestampSchema
            The schema of the reference datetime column.
        time_interval: TimeInterval
            Specifies the time interval for the time series. Note that only intervals defined with a single time unit
            (e.g., 1 hour, 1 day) are supported.
        series_id_column: Optional[str]
            The column that represents the unique identifier for each time series.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
        description: Optional[str]
            The optional description for the new table.
        datetime_partition_column: Optional[str]
            The optional column for the datetime column used for partitioning the time series table.
        datetime_partition_schema: Optional[TimestampSchema]
            The optional timestamp schema for the datetime partition column.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            event table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        TimeSeriesTable
            TimeSeriesTable created from the source table.

        Examples
        --------
        Create an time series table from a source table.

        >>> # Register GROCERYSALES as a time series table
        >>> source_table = ds.get_source_table(  # doctest: +SKIP
        ...     database_name="spark_catalog", schema_name="GROCERY", table_name="GROCERYSALES"
        ... )
        >>> sales_table = source_table.create_time_series_table(  # doctest: +SKIP
        ...     name="GROCERYSALES",
        ...     reference_datetime_column="Date",
        ...     reference_datetime_schema=TimestampSchema(timezone="Etc/UTC"),
        ...     time_interval=TimeInterval(value=1, unit="DAY"),
        ...     series_id_column="StoreGuid",
        ...     record_creation_timestamp_column="record_available_at",
        ... )

        See Also
        --------
        - [TimestampSchema](/reference/featurebyte.query_graph.model.timestamp_schema.TimestampSchema/):
            Schema for a timestamp column that can include timezone information.
        - [TimeIntervalUnit](/reference/featurebyte.enum.TimeIntervalUnit/):
            Time interval unit for the time series.
        """

        from featurebyte.api.time_series_table import TimeSeriesTable

        return TimeSeriesTable.create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            reference_datetime_column=reference_datetime_column,
            reference_datetime_schema=reference_datetime_schema,
            datetime_partition_column=datetime_partition_column,
            datetime_partition_schema=datetime_partition_schema,
            time_interval=time_interval,
            series_id_column=series_id_column,
            description=description,
            _id=_id,
        )

    @typechecked
    def get_or_create_event_table(
        self,
        name: str,
        event_timestamp_column: str,
        event_id_column: Optional[str],
        event_timestamp_timezone_offset: Optional[str] = None,
        event_timestamp_timezone_offset_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        event_timestamp_schema: Optional[TimestampSchema] = None,
        datetime_partition_column: Optional[str] = None,
        datetime_partition_schema: Optional[TimestampSchema] = None,
        _id: Optional[ObjectId] = None,
    ) -> EventTable:
        """
        Get or create event table from this source table. Internally, this method calls `EventTable.get` by name,
        if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        event_timestamp_column: str
            The column that contains the timestamp of the associated event.
        event_id_column: Optional[str]
            The column that represents the unique identfier for each event.
        event_timestamp_timezone_offset: Optional[str]
            Timezone offset for the event timestamp column. Supported format is "(+|-)HH:mm".
            Specify this if the timezone offset is a fixed value. Examples: "+08:00" or "-05:00".
        event_timestamp_timezone_offset_column: Optional[str]
            Timezone offset column for the event timestamp column. The column is expected to have
            string type, and each value in the column is expected to have the format "(+|-)HH:mm".
            Specify this if the timezone offset is different for different rows in the event table.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
        description: Optional[str]
            The optional description for the new table.
        event_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the event timestamp column.
        datetime_partition_column: Optional[str]
            The optional column for the datetime column used for partitioning the event table.
        datetime_partition_schema: Optional[TimestampSchema]
            The optional timestamp schema for the datetime partition column.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            event table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        EventTable
        """

        from featurebyte.api.event_table import EventTable

        return EventTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            event_timestamp_column=event_timestamp_column,
            event_id_column=event_id_column,
            event_timestamp_timezone_offset=event_timestamp_timezone_offset,
            event_timestamp_timezone_offset_column=event_timestamp_timezone_offset_column,
            event_timestamp_schema=event_timestamp_schema,
            datetime_partition_column=datetime_partition_column,
            datetime_partition_schema=datetime_partition_schema,
            description=description,
            _id=_id,
        )

    @typechecked
    def get_or_create_item_table(
        self,
        name: str,
        event_id_column: str,
        item_id_column: Optional[str],
        event_table_name: str,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
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
        item_id_column: Optional[str]
            Item ID column from the given source table.
        event_table_name: str
            Name of the EventTable associated with this ItemTable.
        record_creation_timestamp_column: Optional[str]
            Record creation timestamp column from the given source table.
        description: Optional[str]
            The optional description of the table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create an
            item table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        ItemTable
        """

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
            description=description,
            _id=_id,
        )

    @typechecked
    def get_or_create_dimension_table(
        self,
        name: str,
        dimension_id_column: str,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
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
        description: Optional[str]
            The optional description of the table.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            dimension table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        DimensionTable
        """

        from featurebyte.api.dimension_table import DimensionTable

        return DimensionTable.get_or_create(
            source_table=self,
            name=name,
            record_creation_timestamp_column=record_creation_timestamp_column,
            dimension_id_column=dimension_id_column,
            description=description,
            _id=_id,
        )

    @typechecked
    def get_or_create_scd_table(
        self,
        name: str,
        natural_key_column: Optional[str],
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        effective_timestamp_schema: Optional[TimestampSchema] = None,
        end_timestamp_schema: Optional[TimestampSchema] = None,
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
        description: Optional[str]
            The optional description of the table.
        effective_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the effective timestamp column.
        end_timestamp_schema: Optional[TimestampSchema]
            The optional timestamp schema for the end timestamp column.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            SCD table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        SCDTable
        """

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
            description=description,
            effective_timestamp_schema=effective_timestamp_schema,
            end_timestamp_schema=end_timestamp_schema,
        )

    @typechecked
    def get_or_create_time_series_table(
        self,
        name: str,
        reference_datetime_column: str,
        reference_datetime_schema: TimestampSchema,
        time_interval: TimeInterval,
        series_id_column: Optional[str],
        record_creation_timestamp_column: Optional[str] = None,
        description: Optional[str] = None,
        datetime_partition_column: Optional[str] = None,
        datetime_partition_schema: Optional[TimestampSchema] = None,
        _id: Optional[ObjectId] = None,
    ) -> TimeSeriesTable:
        """
        Get or create time series table from this source table. Internally, this method calls `TimeSeriesTable.get` by name,
        if the table does not exist, it will be created.

        Parameters
        ----------
        name: str
            The desired name for the new table.
        reference_datetime_column: str
            The column that contains the reference datetime of the associated time series.
        reference_datetime_schema: TimestampSchema
            The schema of the reference datetime column.
        time_interval: TimeInterval
            The time interval of the time series.
        series_id_column: Optional[str]
            The column that represents the unique identifier for each time series.
        record_creation_timestamp_column: str
            The optional column for the timestamp when a record was created.
        description: Optional[str]
            The optional description of the table.
        datetime_partition_column: Optional[str]
            The optional column for the datetime column used for partitioning the time series table.
        datetime_partition_schema: Optional[TimestampSchema]
            The optional timestamp schema for the datetime partition column.
        _id: Optional[ObjectId]
            Identity value for constructed object. This should only be used for cases where we want to create a
            time series table with a specific ID. This should not be a common operation, and is typically used in tests
            only.

        Returns
        -------
        TimeSeriesTable
        """

        from featurebyte.api.time_series_table import TimeSeriesTable

        return TimeSeriesTable.get_or_create(
            source_table=self,
            name=name,
            reference_datetime_column=reference_datetime_column,
            reference_datetime_schema=reference_datetime_schema,
            datetime_partition_column=datetime_partition_column,
            datetime_partition_schema=datetime_partition_schema,
            time_interval=time_interval,
            series_id_column=series_id_column,
            record_creation_timestamp_column=record_creation_timestamp_column,
            description=description,
            _id=_id,
        )

    def create_observation_table(
        self,
        name: str,
        sample_rows: Optional[int] = None,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
        context_name: Optional[str] = None,
        skip_entity_validation_checks: Optional[bool] = False,
        primary_entities: Optional[List[str]] = None,
        target_column: Optional[str] = None,
        sample_from_timestamp: Optional[Union[datetime, str]] = None,
        sample_to_timestamp: Optional[Union[datetime, str]] = None,
    ) -> ObservationTable:
        """
        Creates an ObservationTable from the SourceTable.

        When you specify the columns and the columns_rename_mapping parameters, make sure that the table has:

        - column(s) containing entity values with an accepted serving name.
        - a column containing historical points-in-time in UTC. The column name must be "POINT_IN_TIME".

        Parameters
        ----------
        name: str
            Observation table name.
        sample_rows: Optional[int]
            Optionally sample the source table to this number of rows before creating the
            observation table.
        columns: Optional[list[str]]
            Include only these columns when creating the observation table. If None, all columns are
            included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the source table using this mapping from old column names to new
            column names when creating the observation table. If None, no columns are renamed.
        context_name: Optional[str]
            Context name for the observation table.
        skip_entity_validation_checks: Optional[bool]
            Skip entity validation checks when creating the observation table.
        primary_entities: Optional[List[str]]
            List of primary entities for the observation table.
        target_column: Optional[str]
            Name of the column in the observation table that stores the target values.
            The target column name must match an existing target namespace in the catalog.
            The data type and primary entities must match the those in the target namespace.
        sample_from_timestamp: Optional[Union[datetime, str]]
            Start of date range to sample from.
        sample_to_timestamp: Optional[Union[datetime, str]]
            End of date range to sample from.

        Returns
        -------
        ObservationTable

        Examples
        --------
        >>> ds = fb.FeatureStore.get(<feature_store_name>).get_data_source()  # doctest: +SKIP
        >>> source_table = ds.get_source_table(  # doctest: +SKIP
        ...   database_name="<data_base_name>",
        ...   schema_name="<schema_name>",
        ...   table_name=<table_name>
        ... )
        >>> observation_table = source_table.create_observation_table(  # doctest: +SKIP
        ...   name="<observation_table_name>",
        ...   sample_rows=desired_sample_size,
        ...   columns=[<timestamp_column_name>, <entity_column_name>],
        ...   columns_rename_mapping={
        ...     timestamp_column_name: "POINT_IN_TIME",
        ...     entity_column_name: <entity_serving_name>,
        ...   },
        ...   context_id=context_id,
        ... )
        """

        from featurebyte.api.context import Context
        from featurebyte.api.observation_table import ObservationTable

        context_id = Context.get(context_name).id if context_name else None
        primary_entity_ids = []
        if primary_entities is not None:
            for entity_name in primary_entities:
                primary_entity_ids.append(Entity.get(entity_name).id)
        else:
            logger.warning("Primary entities will be a mandatory parameter in SDK version 0.7.")

        # Validate timestamp inputs
        sample_from_timestamp = (
            validate_datetime_input(sample_from_timestamp) if sample_from_timestamp else None
        )
        sample_to_timestamp = (
            validate_datetime_input(sample_to_timestamp) if sample_to_timestamp else None
        )

        payload = ObservationTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=SourceTableObservationInput(
                source=self.tabular_source,
                columns=columns,
                columns_rename_mapping=columns_rename_mapping,
            ),
            sample_rows=sample_rows,
            context_id=context_id,
            skip_entity_validation_checks=skip_entity_validation_checks,
            primary_entity_ids=primary_entity_ids,
            target_column=target_column,
            sample_from_timestamp=sample_from_timestamp,
            sample_to_timestamp=sample_to_timestamp,
        )
        observation_table_doc = ObservationTable.post_async_task(
            route="/observation_table", payload=payload.json_dict()
        )
        return ObservationTable.get_by_id(observation_table_doc["_id"])

    def get_batch_request_input(
        self,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> BatchRequestInput:
        """
        Get a BatchRequestInput object for the SourceTable.

        Parameters
        ----------
        columns: Optional[list[str]]
            Include only these columns in the view for the batch request input. If None,
            all columns are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the view using this mapping from old column names to new column names
            for the batch request input. If None, no columns are renamed.

        Returns
        -------
        BatchRequestInput
            BatchRequestInput object.
        """
        return SourceTableBatchRequestInput(
            source=self.tabular_source,
            columns=columns,
            columns_rename_mapping=columns_rename_mapping,
        )

    def create_batch_request_table(
        self,
        name: str,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> BatchRequestTable:
        """
        Creates an BatchRequestTable from the SourceTable.

        When you specify the columns and the columns_rename_mapping parameters, make sure that the table has a column
        containing entity values with an accepted serving name.

        Parameters
        ----------
        name: str
            Batch request table name.
        columns: Optional[list[str]]
            Include only these columns when creating the batch request table. If None, all columns
            are included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the source table using this mapping from old column names to new
            column names when creating the batch request table. If None, no columns are renamed.

        Returns
        -------
        BatchRequestTable

        Examples
        --------
        >>> data_source = fb.FeatureStore.get(<feature_store_name>).get_data_source()  # doctest: +SKIP
        >>> source_table = data_source.get_source_table(  # doctest: +SKIP
        ...   database_name="<data_base_name>",
        ...   schema_name="<schema_name>",
        ...   table_name=<table_name>
        ... )
        >>> batch_request_table = source_table.create_batch_request_table(  # doctest: +SKIP
        ...   name="<batch_request_table_name>",
        ...   columns=[<entity_column_name>],
        ...   columns_rename_mapping={ <entity_column_name>: <entity_serving_name>, }
        ... )
        """

        from featurebyte.api.batch_request_table import BatchRequestTable

        payload = BatchRequestTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=self.get_batch_request_input(
                columns=columns, columns_rename_mapping=columns_rename_mapping
            ),
        )
        batch_request_table_doc = BatchRequestTable.post_async_task(
            route="/batch_request_table", payload=payload.json_dict()
        )
        return BatchRequestTable.get_by_id(batch_request_table_doc["_id"])

    def create_static_source_table(
        self,
        name: str,
        sample_rows: Optional[int] = None,
        columns: Optional[list[str]] = None,
        columns_rename_mapping: Optional[dict[str, str]] = None,
    ) -> StaticSourceTable:
        """
        Creates a StaticSourceTable from the SourceTable.

        Parameters
        ----------
        name: str
            Static source table name.
        sample_rows: Optional[int]
            Optionally sample the source table to this number of rows before creating the
            static source table.
        columns: Optional[list[str]]
            Include only these columns when creating the static source table. If None, all columns are
            included.
        columns_rename_mapping: Optional[dict[str, str]]
            Rename columns in the source table using this mapping from old column names to new
            column names when creating the static source table. If None, no columns are renamed.

        Returns
        -------
        StaticSourceTable

        Examples
        --------
        >>> ds = fb.FeatureStore.get(<feature_store_name>).get_data_source()  # doctest: +SKIP
        >>> source_table = ds.get_source_table(  # doctest: +SKIP
        ...   database_name="<data_base_name>",
        ...   schema_name="<schema_name>",
        ...   table_name=<table_name>
        ... )
        >>> static_source_table = source_table.create_static_source_table(  # doctest: +SKIP
        ...     name="<static_source_table_name>",
        ...     sample_rows=desired_sample_size,
        ... )
        """

        from featurebyte.api.static_source_table import StaticSourceTable

        payload = StaticSourceTableCreate(
            name=name,
            feature_store_id=self.feature_store.id,
            request_input=SourceTableStaticSourceInput(
                source=self.tabular_source,
                columns=columns,
                columns_rename_mapping=columns_rename_mapping,
            ),
            sample_rows=sample_rows,
        )
        static_source_table_doc = StaticSourceTable.post_async_task(
            route="/static_source_table", payload=payload.json_dict()
        )
        return StaticSourceTable.get_by_id(static_source_table_doc["_id"])

    @perf_logging
    @typechecked
    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Retrieve a preview of the source table / column.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the data.

        Raises
        ------
        RecordRetrievalException
            Preview request failed.

        Examples
        --------
        Preview 3 rows of a source table.
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> source_table = data_source.get_source_table(
        ...     table_name="groceryinvoice",
        ...     database_name="spark_catalog",
        ...     schema_name="doctest_grocery",
        ... )
        >>> source_table.preview(3)
                             GroceryInvoiceGuid                   GroceryCustomerGuid           Timestamp record_available_at  Amount
        0  4fccfb1d-02b3-4047-87ab-4e5f910ccdd1  a7ada4a3-fd92-44e6-a232-175c90b1c939 2022-01-03 12:28:58 2022-01-03 13:01:00   10.68
        1  9cf3c416-7b38-401e-adf6-1bd26650d1d6  a7ada4a3-fd92-44e6-a232-175c90b1c939 2022-01-03 16:32:15 2022-01-03 17:01:00   38.04
        2  0a5b99b2-9ff1-452a-a06e-669e8ed4a9fa  a7ada4a3-fd92-44e6-a232-175c90b1c939 2022-01-07 16:20:04 2022-01-07 17:01:00    1.99
        """

        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/table_preview?limit={limit}",
            json=self.table_data.tabular_source.json_dict(),
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return dataframe_from_json(response.json())

    @perf_logging
    @typechecked
    def shape(self) -> Tuple[int, int]:
        """
        Return the shape of the source table

        Returns
        -------
        Tuple[int, int]

        Raises
        ------
        RecordRetrievalException
            Shape request failed.

        Examples
        --------
        Get the shape of a source table.
        >>> data_source = fb.FeatureStore.get("playground").get_data_source()
        >>> source_table = data_source.get_source_table(
        ...     table_name="groceryinvoice",
        ...     database_name="spark_catalog",
        ...     schema_name="doctest_grocery",
        ... )
        >>> source_table.shape()
        (38107, 5)
        """
        client = Configurations().get_client()
        response = client.post(
            url="/feature_store/table_shape", json=self.table_data.tabular_source.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        shape = FeatureStoreShape(**response.json())
        return shape.num_rows, shape.num_cols

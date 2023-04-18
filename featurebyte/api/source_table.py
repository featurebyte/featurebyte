"""
SourceTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, List, Optional, Tuple, Type, TypeVar, Union, cast

from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.table_data import AbstractTableData
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


class SourceTable(AbstractTableData):
    """
    SourceTable class to preview table.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.SourceTable")

    _table_data_class: ClassVar[Type[AllTableDataT]] = SouceTableData

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

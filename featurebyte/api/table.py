"""
Table class
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict

from bson import ObjectId

from featurebyte.api.base_table import TableListMixin
from featurebyte.api.dimension_table import DimensionTable
from featurebyte.api.event_table import EventTable
from featurebyte.api.item_table import ItemTable
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.time_series_table import TimeSeriesTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.proxy_table import ProxyTableModel


class Table(TableListMixin):
    """
    A Table serves as a logical representation of a source table within the Catalog. The Table does not store data
    itself, but instead provides a way to access the source table and centralize essential metadata for feature
    engineering.

    The metadata on the table’s type determines the types of feature engineering operations that are possible and
    enforce guardrails accordingly. Additional metadata can be optionally incorporated to further aid feature
    engineering. This can include identifying columns that identify or reference entities, providing information
    about the semantics of the table columns, specifying default cleaning operations, or furnishing descriptions of
    both the table and its columns.

    At present, FeatureByte recognizes four table types:

    - Event Table
    - Item Table
    - Slowly Changing Dimension Table
    - Dimension Table.

    Two additional table types, Regular Time Series and Sensor data, will be supported in the near future.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Table")
    _get_schema: ClassVar[Any] = ProxyTableModel
    _data_type_to_cls_mapping: ClassVar[Dict[TableDataType, Any]] = {
        TableDataType.EVENT_TABLE: EventTable,
        TableDataType.ITEM_TABLE: ItemTable,
        TableDataType.SCD_TABLE: SCDTable,
        TableDataType.DIMENSION_TABLE: DimensionTable,
        TableDataType.TIME_SERIES_TABLE: TimeSeriesTable,
    }

    @classmethod
    def get(cls: Any, name: str) -> Any:
        """
        Retrieve saved source table by name

        Parameters
        ----------
        name: str
            Source table name

        Returns
        -------
        Any
            Retrieved source table
        """
        data = cls._get(name)
        data_class = cls._data_type_to_cls_mapping[data.cached_model.type]
        return data_class.get(name)

    @classmethod
    def get_by_id(cls: Any, id: ObjectId) -> Any:
        """
        Retrieve saved source table by ID

        Parameters
        ----------
        id: ObjectId
            Source Table ID

        Returns
        -------
        Any
            Retrieved table source
        """
        data = cls._get_by_id(id)
        data_class = cls._data_type_to_cls_mapping[data.cached_model.type]
        return data_class.get_by_id(id)

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of a Table object, depending on its type.
        The dictionary contains the following common keys:

        - `name`: The name of the Table object.
        - `created_at`: The timestamp indicating when the Table object was created.
        - `updated_at`: The timestamp indicating when the Table object was last updated.
        - `status`: The status of the Table object.
        - `catalog_name`: The catalog name of the Table object.
        - `column_count`: The count of columns in the Table object.
        - `record_creation_timestamp_column`: The name of the column in the Table object that represents the timestamp
            for record creation.
        - `entities`: Details of the entities represented in the Table object.
        - `table_details`: Details of the source table the Table object is connected to.
        - `columns_info`: List of specifications for the columns in the Table object.

        Additional keys are provided for each specific type of Table object:

        For a DimensionTable object:

        - `dimension_id_column`: The primary key of the dimension table.

        For an SCDTable object:

        - `natural_key_column`: The name of the natural key of the SCD table.
        - `effective_timestamp_column`: The name of the column representing the effective timestamp.
        - `surrogate_key_column`: The name of the surrogate key.
        - `end_timestamp_column`: The name of the column representing the end (or expiration) timestamp.
        - `current_flag_column`: The name of the column representing the current flag.

        For an EventTable object:

        - `event_timestamp_column`: The name of the column representing the event timestamp.
        - `event_id_column`: The name of the column representing the event key.
        - `default_feature_job_setting`: The default FeatureJob setting.

        For an ItemTable object:

        - `event_id_column`: The name of the column representing the event key.
        - `item_id_column`: The name of the column representing the item key.
        - `event_table_name`: The name of the EventTable object that the ItemTable is associated with.

        Parameters
        ----------
        verbose: bool
            Controls whether the summary should include information for each column in the table.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.
        """
        return super().info(verbose)

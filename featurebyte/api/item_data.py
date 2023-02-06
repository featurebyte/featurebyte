"""
ItemData class
"""
from __future__ import annotations

from typing import Any, ClassVar, Literal, Optional, Type

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.event_data import EventData
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_store import FrozenDataModel
from featurebyte.models.validator import construct_data_model_root_validator
from featurebyte.query_graph.model.table import AllTableDataT, ItemTableData
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate


class ItemData(FrozenDataModel, DataApiObject):
    """
    ItemData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.ItemData")

    # class variables
    _route = "/item_data"
    _update_schema_class = ItemDataUpdate
    _create_schema_class = ItemDataCreate
    _table_data_class: ClassVar[Type[AllTableDataT]] = ItemTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.ITEM_DATA] = Field(TableDataType.ITEM_DATA, const=True)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        exclude=True, allow_mutation=False
    )

    # pydantic instance variable (internal use)
    int_event_id_column: StrictStr = Field(alias="event_id_column")
    int_item_id_column: StrictStr = Field(alias="item_id_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="int_columns_info",
            expected_column_field_name_type_pairs=[
                ("int_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("int_event_id_column", DBVarType.supported_id_types()),
                ("int_item_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @root_validator(pre=True)
    @classmethod
    def _set_default_feature_job_setting(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "event_data_id" in values:
            event_data_id = values["event_data_id"]
            try:
                default_feature_job_setting = EventData.get_by_id(
                    event_data_id
                ).default_feature_job_setting
            except RecordRetrievalException:
                # Typically this shouldn't happen since event_data_id should be available if the
                # ItemData was instantiated correctly. Currently, this occurs only in tests.
                return values
            values["default_feature_job_setting"] = default_feature_job_setting
        return values

    @property
    def event_id_column(self) -> str:
        """
        Event ID column name of the EventData associated with the ItemData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.event_id_column
        except RecordRetrievalException:
            return self.int_event_id_column

    @property
    def item_id_column(self) -> str:
        """
        Item ID column name of the ItemData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.item_id_column
        except RecordRetrievalException:
            return self.int_item_id_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        event_id_column: str,
        item_id_column: str,
        event_data_name: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> ItemData:
        """
        Create ItemData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Item data name
        event_id_column: str
            Event ID column from the given tabular source
        item_id_column: str
            Item ID column from the given tabular source
        event_data_name: str
            Name of the EventData associated with this ItemData
        record_creation_date_column: Optional[str]
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        EventData

        Raises
        ------
        ValueError
            If the associated EventData does not have event_id_column defined
        """
        event_data = EventData.get(event_data_name)
        if event_data.event_id_column is None:
            raise ValueError("EventData without event_id_column is not supported")
        event_data_id = event_data.id
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_data_id=event_data_id,
        )

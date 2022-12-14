"""
ItemData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson.objectid import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.event_data import EventData
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate


class ItemData(ItemDataModel, DataApiObject):
    """
    ItemData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["Data"],
        proxy_class="featurebyte.ItemData",
    )

    _route = "/item_data"
    _update_schema_class = ItemDataUpdate
    _create_schema_class = ItemDataCreate

    default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        exclude=True, allow_mutation=False
    )

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
            Event data name
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

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Provide basic info for item data.

        Parameters
        ----------
        verbose: bool
            This is a no-op for now. This will be used when we add more functionality to this funciton.

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "name": self.name,
            "record_creation_date_column": self.record_creation_date_column,
            "updated_at": self.updated_at,
            "status": self.status,
            "tabular_source": self.tabular_source,
            "event_id_column": self.event_id_column,
            "item_id_column": self.item_id_column,
            "entities": self.entity_ids,
        }

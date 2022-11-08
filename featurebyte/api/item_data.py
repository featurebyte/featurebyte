"""
ItemData class
"""
from __future__ import annotations

from typing import Any, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.event_data import EventData
from featurebyte.config import Configurations
from featurebyte.enum import TableDataType
from featurebyte.exception import DuplicatedRecordException, RecordRetrievalException
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate


class ItemData(ItemDataModel, DataApiObject):
    """
    ItemData class
    """

    _route = "/item_data"
    _update_schema_class = ItemDataUpdate
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(
        exclude=True, allow_mutation=False
    )

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # the key `_id` is used during deserialization, the key `id` is used during setattr
        return {
            "type": TableDataType.ITEM_DATA,
            "id": values.get("_id", values.get("id")),
        }

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
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        event_data_id = EventData.get(event_data_name).id
        data = ItemDataCreate(
            _id=_id or ObjectId(),
            name=name,
            tabular_source=tabular_source.tabular_source,
            columns_info=tabular_source.columns_info,
            event_id_column=event_id_column,
            item_id_column=item_id_column,
            event_data_id=event_data_id,
            record_creation_date_column=record_creation_date_column,
        )
        client = Configurations().get_client()
        response = client.get(url=cls._route, params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return ItemData(
                    **data.json_dict(),
                    feature_store=tabular_source.feature_store,
                )
            raise DuplicatedRecordException(
                response, f'ItemData (item_data.name: "{name}") exists in saved record.'
            )
        raise RecordRetrievalException(response)

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

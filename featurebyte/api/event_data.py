"""
EventData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import validator
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.util import get_entity
from featurebyte.common.model_util import validate_job_setting_parameters
from featurebyte.config import Configurations, Credentials
from featurebyte.core.mixin import GetAttrMixin
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.models.event_data import EventDataModel, FeatureJobSetting
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate


class EventDataColumn:
    """
    EventDataColumn class to set metadata like entity
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, event_data: EventData, column_name: str) -> None:
        self.event_data = event_data
        self.column_name = column_name

    @typechecked
    def as_entity(self, entity_name: Optional[str]) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: Optional[str]
            Associate column name to the entity, remove association if entity name is None

        Raises
        ------
        RecordUpdateException
            When unexpected record update failure
        """
        column_entity_map = self.event_data.column_entity_map or {}
        if entity_name is None:
            column_entity_map.pop(self.column_name, None)
        else:
            entity_dict = get_entity(entity_name)
            column_entity_map[self.column_name] = entity_dict["_id"]

        data = EventDataUpdate(column_entity_map=column_entity_map)
        client = Configurations().get_client()
        response = client.patch(url=f"/event_data/{self.event_data.id}", json=data.json_dict())
        if response.status_code == HTTPStatus.OK:
            EventData.__init__(
                self.event_data,
                **response.json(),
                feature_store=self.event_data.feature_store,
                credentials=self.event_data.credentials,
                saved=True,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self.event_data.column_entity_map = column_entity_map
        else:
            raise RecordUpdateException(response)


class EventData(EventDataModel, DatabaseTable, ApiObject, GetAttrMixin):
    """
    EventData class
    """

    # class variables
    _route = "/event_data"

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store, "credentials": self.credentials}

    def _get_create_payload(self) -> dict[str, Any]:
        data = EventDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        return {"timestamp": values["event_timestamp_column"]}

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        event_timestamp_column: str,
        record_creation_date_column: Optional[str] = None,
        credentials: Optional[Credentials] = None,
    ) -> EventData:
        """
        Create EventData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Event data name
        event_timestamp_column: str
            Event timestamp column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        credentials: Optional[Credentials]
            Credentials dictionary mapping from the config file

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
        data = EventDataCreate(
            _id=ObjectId(),
            name=name,
            tabular_source=tabular_source.tabular_source,
            event_timestamp_column=event_timestamp_column,
            record_creation_date_column=record_creation_date_column,
        )
        client = Configurations().get_client()
        response = client.get(url="/event_data/", params={"name": name})
        if response.status_code == HTTPStatus.OK:
            response_dict = response.json()
            if not response_dict["data"]:
                return EventData(
                    **data.dict(),
                    feature_store=tabular_source.feature_store,
                    credentials=credentials,
                )
            raise DuplicatedRecordException(
                response, f'EventData (event_data.name: "{name}") exists in saved record.'
            )
        raise RecordRetrievalException(response)

    @validator("event_timestamp_column")
    @classmethod
    def _check_event_timestamp_column_exists(cls, value: str, values: dict[str, Any]) -> str:
        if value not in values["column_var_type_map"]:
            raise ValueError(f'Column "{value}" not found in the table!')
        return value

    @validator("record_creation_date_column")
    @classmethod
    def _check_record_creation_date_column_exists(cls, value: str, values: dict[str, Any]) -> str:
        if value and value not in values["column_var_type_map"]:
            raise ValueError(f'Column "{value}" not found in the table!')
        return value

    @typechecked
    def __getitem__(self, item: str) -> EventDataColumn:
        """
        Retrieve column from the table

        Parameters
        ----------
        item: str
            Column name

        Returns
        -------
        EventDataColumn

        Raises
        ------
        KeyError
            when accessing non-exist column
        """
        if item not in self.column_var_type_map:
            raise KeyError(f'Column "{item}" does not exist!')
        return EventDataColumn(event_data=self, column_name=item)

    def info(self) -> dict[str, Any]:
        """
        Retrieve object info

        If the object is not saved at persistent layer, object at memory info is retrieved.
        Otherwise, object info at persistent layer is retrieved (local object will be overridden).

        Returns
        -------
        dict[str, Any]
            Saved object stored at persistent layer

        Raises
        ------
        RecordRetrievalException
            When unexpected retrieval failure
        """
        client = Configurations().get_client()
        response = client.get(url=f"/event_data/{self.id}")
        if response.status_code == HTTPStatus.OK:
            type(self).__init__(
                self,
                **response.json(),
                feature_store=self.feature_store,
                credentials=self.credentials,
                saved=True,
            )
            return self.dict()
        if response.status_code == HTTPStatus.NOT_FOUND:
            return self.dict()
        raise RecordRetrievalException(response)

    @typechecked
    def update_default_feature_job_setting(
        self, feature_job_setting: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: Optional[Dict[str, Any]]
            Feature job setting dictionary contains blind_spot, frequency & time_modulo_frequency keys

        Raises
        ------
        RecordUpdateException
            When unexpected record update failure
        """
        client = Configurations().get_client()
        if feature_job_setting:
            _ = validate_job_setting_parameters(**feature_job_setting)
        else:
            # create feature job setting analysis async task
            data = client.post(
                url="/feature_job_setting_analysis", json={"event_data_id": str(self.id)}
            )

        import pdb

        pdb.set_trace()

        feature_job_setting = FeatureJobSetting(**feature_job_setting)
        data = EventDataUpdate(default_feature_job_setting=feature_job_setting)
        response = client.patch(url=f"/event_data/{self.id}", json=data.dict())
        if response.status_code == HTTPStatus.OK:
            type(self).__init__(
                self,
                **response.json(),
                feature_store=self.feature_store,
                credentials=self.credentials,
                saved=True,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self.default_feature_job_setting = data.default_feature_job_setting
        else:
            raise RecordUpdateException(response)

    @property
    def default_feature_job_setting_history(self) -> list[dict[str, Any]]:
        """
        List of default_job_setting history entries

        Returns
        -------
        list[dict[str, Any]]

        Raises
        ------
        RecordRetrievalException
            When unexpected retrieval failure
        """
        client = Configurations().get_client()
        response = client.get(url=f"/event_data/history/default_feature_job_setting/{self.id}")
        if response.status_code == HTTPStatus.OK:
            history: list[dict[str, Any]] = response.json()
            return history
        raise RecordRetrievalException(response)

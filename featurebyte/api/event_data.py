"""
EventData class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import validator

from featurebyte.api.api_object import ApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.util import get_entity
from featurebyte.config import Configurations, Credentials
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

    def as_entity(self, entity_name: str | None) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: str | None
            Associate column name to the entity, remove association if entity name is None

        Raises
        ------
        TypeError
            When the entity name has non-string type
        RecordUpdateException
            When unexpected record update failure
        """
        column_entity_map = self.event_data.column_entity_map or {}
        if entity_name is None:
            column_entity_map.pop(self.column_name, None)
        elif isinstance(entity_name, str):
            entity_dict = get_entity(entity_name)
            column_entity_map[self.column_name] = entity_dict["_id"]
        else:
            raise TypeError(f'Unsupported type "{type(entity_name)}" for tag name "{entity_name}"!')

        data = EventDataUpdate(column_entity_map=column_entity_map)
        client = Configurations().get_client()
        response = client.patch(url=f"/event_data/{self.event_data.id}", json=data.json_dict())
        if response.status_code == HTTPStatus.OK:
            EventData.__init__(
                self.event_data,
                **response.json(),
                feature_store=self.event_data.feature_store,
                credentials=self.event_data.credentials,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self.event_data.column_entity_map = column_entity_map
        else:
            raise RecordUpdateException(response)


class EventData(EventDataModel, DatabaseTable, ApiObject):
    """
    EventData class
    """

    # class variables
    _route = "/event_data"

    def _get_init_params(self) -> dict[str, Any]:
        return {"feature_store": self.feature_store, "credentials": self.credentials}

    def _get_create_payload(self) -> dict[str, Any]:
        data = EventDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        return {"timestamp": values["event_timestamp_column"]}

    @classmethod
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        event_timestamp_column: str,
        record_creation_date_column: str | None = None,
        credentials: Credentials | None = None,
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
        credentials: Credentials | None
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

    def __getattr__(self, item: str) -> EventDataColumn:
        return self.__getitem__(item)

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
            )
            return self.dict()
        if response.status_code == HTTPStatus.NOT_FOUND:
            return self.dict()
        raise RecordRetrievalException(response)

    def update_default_feature_job_setting(
        self, blind_spot: str, frequency: str, time_modulo_frequency: str
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        blind_spot: str
            Blind spot parameter
        frequency: str
            Frequency parameter
        time_modulo_frequency: str
            Time modulo frequency

        Raises
        ------
        RecordUpdateException
            When unexpected record update failure
        """
        feature_job_setting = FeatureJobSetting(
            blind_spot=blind_spot,
            frequency=frequency,
            time_modulo_frequency=time_modulo_frequency,
        )
        data = EventDataUpdate(default_feature_job_setting=feature_job_setting)
        client = Configurations().get_client()
        response = client.patch(url=f"/event_data/{self.id}", json=data.dict())
        if response.status_code == HTTPStatus.OK:
            type(self).__init__(
                self,
                **response.json(),
                feature_store=self.feature_store,
                credentials=self.credentials,
            )
        elif response.status_code == HTTPStatus.NOT_FOUND:
            self.default_feature_job_setting = data.default_feature_job_setting
        else:
            raise RecordUpdateException(response)

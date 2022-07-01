"""
EventData class
"""
from __future__ import annotations

from typing import Any

import json
from http import HTTPStatus

from pydantic import validator

from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.logger import logger
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails


class EventDataColumn:
    """
    EventDataColumn class to set metadata like entity
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, event_data: EventData, column_name: str) -> None:
        self.event_data = event_data
        self.column_name = column_name

    def as_entity(self, tag_name: str) -> None:
        """
        Set the column name as entity with tag name

        Parameters
        ----------
        tag_name: str
            Tag name of the entity

        Raises
        ------
        TypeError
            When the tag name has non-string type
        """
        if isinstance(tag_name, str):
            self.event_data.column_entity_map[self.column_name] = tag_name
        else:
            raise TypeError(f'Unsupported type "{type(tag_name)}" for tag name "{tag_name}"!')


class EventData(EventDataModel, DatabaseTable):
    """
    EventData class
    """

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
        credentials: dict[FeatureStoreModel, Credential | None] | None = None,
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
        credentials: dict[FeatureStoreModel, Credential | None] | None
            Credentials dictionary mapping from the config file

        Returns
        -------
        EventData
        """
        node_parameters = tabular_source.node.parameters.copy()
        database_source = FeatureStore(**node_parameters["database_source"])
        table_details = TableDetails(**node_parameters["dbtable"])
        return EventData(
            name=name,
            tabular_source=(database_source, table_details),
            event_timestamp_column=event_timestamp_column,
            record_creation_date_column=record_creation_date_column,
            credentials=credentials,
        )

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

    def _update_state(self, model: EventDataModel, keys: list[str], log_message: str) -> None:
        """
        Update current object state based on the response

        Parameters
        ----------
        model: EventDataModel
            Response of the request (in EventDataModel)
        keys: list[str]
            List of keys to update
        log_message: str
            Log message
        """
        updated_map = {}
        for key in keys:
            original_value = getattr(self, key)
            value = getattr(model, key)
            if original_value != value:
                setattr(self, key, value)
                updated_map[key] = (original_value, value)
        logger.debug(log_message, extra=updated_map)

    def save_as_draft(self) -> None:
        """
        Save event data to persistent as draft

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordCreationException
            When fail to save the event data (general failure)
        """
        client = Configurations().get_client()
        payload = json.loads(self.json())
        response = client.post(url="/event_data", json=payload)
        if response.status_code != HTTPStatus.CREATED:
            if response.status_code == HTTPStatus.CONFLICT:
                raise DuplicatedRecordException(response)
            raise RecordCreationException(response)
        event_data_model = EventDataModel.parse_obj(response.json())
        self._update_state(
            event_data_model,
            keys=["created_at", "status"],
            log_message="create an EventData object",
        )

    def publish(self) -> None:
        """
        Save event data to persistent as publish

        Raises
        ------
        RecordUpdateException
            When fail to update the saved event data
        """
        try:
            self.save_as_draft()
        except DuplicatedRecordException:
            pass

        client = Configurations().get_client()
        payload = json.loads(self.json())
        payload["status"] = EventDataStatus.PUBLISHED
        response = client.patch(url=f'/event_data/{payload["name"]}', json=payload)
        if response.status_code != HTTPStatus.OK:
            raise RecordUpdateException(response)
        event_data_model = EventDataModel.parse_obj(response.json())
        self._update_state(
            event_data_model, keys=["status"], log_message="update an EventData object"
        )

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
        response = client.get(url=f"/event_data/{self.name}")
        if response.status_code == HTTPStatus.OK:
            loaded_event_data = EventData.parse_obj(response.json())
            self._update_state(
                loaded_event_data,
                keys=list(self.dict().keys()),
                log_message="EventData object call info",
            )
            return self.dict()
        if response.status_code == HTTPStatus.NOT_FOUND:
            return self.dict()
        raise RecordRetrievalException(response)

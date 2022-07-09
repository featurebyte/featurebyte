"""
EventData class
"""
from __future__ import annotations

from typing import Any, Tuple

import json
from http import HTTPStatus

from bson.objectid import ObjectId
from pydantic import validator

from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.util import convert_response_to_dict, get_entity
from featurebyte.config import Configurations
from featurebyte.exception import (
    DuplicatedRecordException,
    RecordCreationException,
    RecordRetrievalException,
)
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.schema.event_data import EventDataCreate


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
        Set the column name as entity with tag name

        Parameters
        ----------
        entity_name: str | None
            Associate column name to the entity, remove association if entity name is None

        Raises
        ------
        ValueError
            When the entity name is not found
        TypeError
            When the entity name has non-string type
        """
        if entity_name is None:
            self.event_data.column_entity_map.pop(self.column_name, None)
        elif isinstance(entity_name, str):
            entity_dict = get_entity(entity_name)
            if entity_dict:
                self.event_data.column_entity_map[self.column_name] = entity_dict["id"]
                return
            raise ValueError(f'Entity name "{entity_name}" not found!')
        else:
            raise TypeError(f'Unsupported type "{type(entity_name)}" for tag name "{entity_name}"!')


class EventData(EventDataModel, DatabaseTable):
    """
    EventData class
    """

    tabular_source: Tuple[FeatureStore, TableDetails]

    class Config:
        """
        Pydantic Config class
        """

        # pylint: disable=too-few-public-methods

        fields = {
            "credentials": {"exclude": True},
            "graph": {"exclude": True},
            "node": {"exclude": True},
            "row_index_lineage": {"exclude": True},
            "column_var_type_map": {"exclude": True},
        }
        json_encoders = {ObjectId: str}

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

        Raises
        ------
        DuplicatedRecordException
            When record with the same key exists at the persistent layer
        RecordRetrievalException
            When unexpected retrieval failure
        """
        data = EventDataCreate(
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
                return EventData(**data.dict(), credentials=credentials)
            raise DuplicatedRecordException(
                response, f'EventData name "{name}" exists in saved record.'
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
        super().__init__(**{**convert_response_to_dict(response), "credentials": self.credentials})

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
            super().__init__(
                **{**convert_response_to_dict(response), "credentials": self.credentials}
            )
            return self.dict()
        if response.status_code == HTTPStatus.NOT_FOUND:
            return self.dict()
        raise RecordRetrievalException(response)

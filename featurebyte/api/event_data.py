"""
EventData class
"""
from __future__ import annotations

from typing import Any

from pydantic import validator

from featurebyte.api.database_table import DatabaseTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.util import get_entity
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails


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

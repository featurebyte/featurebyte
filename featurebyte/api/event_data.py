"""
EventData class
"""
from __future__ import annotations

from typing import Any

from pydantic import validator

from featurebyte.api.database_source import DatabaseSource
from featurebyte.api.database_table import DatabaseTable
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import DatabaseSourceModel, EventDataModel


class EventDataColumn:
    """
    EventDataColumn class to set metadata like entity
    """

    # pylint: disable=R0903

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
        """
        self.event_data.column_entity_map[self.column_name] = tag_name


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
        credentials: dict[DatabaseSourceModel, Credential | None] | None = None,
    ) -> EventData:
        """
        Create EventData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from DatabaseSource
        name: str
            Event data name
        event_timestamp_column: str
            Event timestamp column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        credentials: dict[DatabaseSourceModel, Credential | None] | None
            Credentials dictionary mapping from the config file

        Returns
        -------
        EventData
        """
        node_parameters = tabular_source.node.parameters.copy()
        database_source = DatabaseSource(**node_parameters["database_source"])
        table_name = node_parameters["dbtable"]
        return EventData(
            name=name,
            tabular_source=(database_source, table_name),
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
        ValueError
            when accessing non-exist column
        """
        if item not in self.column_var_type_map:
            raise ValueError(f'Column "{item}" not exists!')
        return EventDataColumn(event_data=self, column_name=item)

    def __getattr__(self, item: str) -> EventDataColumn:
        return self.__getitem__(item)

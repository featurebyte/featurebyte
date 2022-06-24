"""
EventData class
"""
from __future__ import annotations

from typing import Any

from featurebyte.api.database_source import DatabaseSource
from featurebyte.api.database_table import DatabaseTable
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import DatabaseSourceModel, EventDataModel


class EventData(EventDataModel, DatabaseTable):
    """
    EventData class
    """

    # pylint: disable=R0903 (too-few-public-methods)

    @classmethod
    def _get_other_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        return {
            "timestamp": values["event_timestamp_column"],
            "record_creation_date": values.get("record_creation_date_column"),
        }

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

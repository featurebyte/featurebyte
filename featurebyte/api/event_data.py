"""
EventData class
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from pydantic import Field, root_validator

from featurebyte.api.database_source import DatabaseSource
from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType
from featurebyte.models.credential import Credential
from featurebyte.models.event_data import (
    DatabaseSourceModel,
    EventDataModel,
    EventDataStatus,
    FeatureJobSettingHistoryEntry,
)
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.manager import SessionManager


class EventData(EventDataModel, DatabaseTable):
    """
    EventData class
    """

    # pylint: disable=R0903 (too-few-public-methods)

    created_at: Optional[datetime] = Field(default=None)
    history: List[FeatureJobSettingHistoryEntry] = Field(default_factory=list)
    status: Optional[EventDataStatus] = Field(default=None)
    column_var_type_map: Dict[str, DBVarType]
    credentials: Optional[Dict[DatabaseSourceModel, Optional[Credential]]] = Field(default=None)

    class Config:
        """
        Pydantic Config class
        """

        fields = {
            "credentials": {"exclude": True},
            "graph": {"exclude": True},
            "node": {"exclude": True},
            "row_index_lineage": {"exclude": True},
            "session": {"exclude": True},
            "column_var_type_map": {"exclude": True},
        }

    @root_validator(pre=True)
    @classmethod
    def _generate_graph_settings(cls, values: dict[str, Any]) -> dict[str, Any]:
        credentials = values.get("credentials")
        if credentials is None:
            config = Configurations()
            credentials = config.credentials

        database_source, table_name = values["tabular_source"]
        if isinstance(database_source, dict):
            database_source = DatabaseSource(**database_source)
        session_manager = SessionManager(credentials=credentials)
        session = session_manager[database_source]
        table_schema = session.list_table_schema(table_name=table_name)

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": list(table_schema.keys()),
                "dbtable": table_name,
                "timestamp": values["event_timestamp_column"],
                "record_creation_date": values.get("record_creation_date_column"),
                "database_source": database_source.dict(),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        values["node"] = node
        values["row_index_lineage"] = (node.name,)
        values["session"] = session
        values["column_var_type_map"] = table_schema
        return values

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

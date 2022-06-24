"""
DatabaseSource class
"""
from __future__ import annotations

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Configurations, Credentials
from featurebyte.models.event_data import DatabaseSourceModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager


class DatabaseSource(DatabaseSourceModel):
    """
    DatabaseSource class
    """

    def get_session(self, credentials: Credentials | None = None) -> BaseSession:
        """
        Get data source session based on provided configuration

        Parameters
        ----------
        credentials: Credentials
            data source to credential mapping used to initiate a new connection

        Returns
        -------
        BaseSession
        """
        if credentials is None:
            config = Configurations()
            credentials = config.credentials
        session_manager = SessionManager(credentials=credentials)
        return session_manager[self]

    def list_tables(self, credentials: Credentials | None = None) -> list[str]:
        """
        List tables of the data source

        Parameters
        ----------
        credentials: Credentials
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(credentials=credentials).list_tables()

    def __getitem__(self, item: str | tuple[str, Credentials]) -> DatabaseTable:
        """
        Get table from the data source

        Parameters
        ----------
        item: str | tuple[str, Credentials]
            Table name or (table name, credentials) pair

        Returns
        -------
        DatabaseTable

        Raises
        ------
        TypeError
            When the item type does not support

        """
        table_name, credentials = "", None
        if isinstance(item, str):
            table_name = item
        elif isinstance(item, tuple):
            table_name, credentials = item
        else:
            raise TypeError(f"DatabaseSource indexing with value '{item}' not supported!")

        table_schema = self.get_session(credentials=credentials).list_table_schema(
            table_name=table_name
        )

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": list(table_schema.keys()),
                "dbtable": table_name,
                "timestamp": None,
                "database_source": self.dict(),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        return DatabaseTable(
            node=node,
            row_index_lineage=(node.name,),
            session=self.get_session(credentials=credentials),
            column_var_type_map=table_schema,
        )

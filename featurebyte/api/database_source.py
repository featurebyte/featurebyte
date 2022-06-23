"""
DatabaseSource class
"""
from __future__ import annotations

from featurebyte.api.database_table import DatabaseTable
from featurebyte.config import Configurations
from featurebyte.models.event_data import DatabaseSourceModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.session.base import BaseSession
from featurebyte.session.manager import SessionManager


class DatabaseSource(DatabaseSourceModel):
    """
    DatabaseSource class
    """

    def get_session(self, config: Configurations | None = None) -> BaseSession:
        """
        Get data source session based on provided configuration

        Parameters
        ----------
        config: Configurations
            configuration contains data source settings & credentials

        Returns
        -------
        BaseSession
        """
        if config is None:
            config = Configurations()
        session_manager = SessionManager(credentials=config.credentials)
        return session_manager[self]

    def list_tables(self, config: Configurations | None = None) -> list[str]:
        """
        List tables of the data source

        Parameters
        ----------
        config: Configurations
            configuration contains data source settings & credentials

        Returns
        -------
        list tables
        """
        return self.get_session(config=config).list_tables()

    def __getitem__(self, item: str | tuple[str, Configurations]) -> DatabaseTable:
        """
        Get table from the data source

        Parameters
        ----------
        item: str
            Table name

        Returns
        -------
        DatabaseTable

        Raises
        ------
        Type
            When the item type does not support

        """
        table_name, config = "", None
        if isinstance(item, str):
            table_name = item
        elif isinstance(item, tuple):
            table_name, config = item
        else:
            raise TypeError(f"DatabaseSource indexing with value '{item}' not supported!")

        table_schema = self.get_session(config=config).list_table_schema(table_name=table_name)

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
            session=self.get_session(config),
            column_var_type_map=table_schema,
        )

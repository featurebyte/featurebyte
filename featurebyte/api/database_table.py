"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd
from pydantic import Field, root_validator

from featurebyte.config import Configurations, Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel, QueryObject
from featurebyte.enum import DBVarType
from featurebyte.models.event_data import DatabaseSourceModel, DatabaseTableModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class DatabaseTable(DatabaseTableModel, QueryObject):
    """
    DatabaseTable class to preview table
    """

    column_var_type_map: Dict[str, DBVarType]
    credentials: Optional[Credentials] = Field(default=None)

    class Config:
        """
        Pydantic Config class
        """

        fields = {
            "credentials": {"exclude": True},
            "graph": {"exclude": True},
            "node": {"exclude": True},
            "row_index_lineage": {"exclude": True},
            "column_var_type_map": {"exclude": True},
        }

    @classmethod
    def _get_other_node_parameters(cls, values):
        _ = values
        return {}

    @root_validator(pre=True)
    @classmethod
    def _set_graph_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        credentials = values.get("credentials")
        if credentials is None:
            config = Configurations()
            credentials = config.credentials

        database_source, table_name = values["tabular_source"]
        if isinstance(database_source, dict):
            database_source = ExtendedDatabaseSourceModel(**database_source)
        elif isinstance(database_source, DatabaseSourceModel):
            database_source = ExtendedDatabaseSourceModel(**database_source.dict())

        session = database_source.get_session(credentials=credentials)
        table_schema = session.list_table_schema(table_name=table_name)

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": list(table_schema.keys()),
                "dbtable": table_name,
                "database_source": database_source.dict(),
                **cls._get_other_node_parameters(values),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        values["node"] = node
        values["row_index_lineage"] = (node.name,)
        values["column_var_type_map"] = table_schema
        return values

    @property
    def dtypes(self) -> pd.Series:
        """
        Get the column datatype info from the table

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

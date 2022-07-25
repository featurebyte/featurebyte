"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import Field, StrictStr, root_validator

from featurebyte.config import Configurations, Credentials
from featurebyte.core.frame import BaseFrame
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.enum import DBVarType
from featurebyte.models.feature_store import DatabaseTableModel, FeatureStoreModel, TableDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class DatabaseTable(DatabaseTableModel, BaseFrame):
    """
    DatabaseTable class to preview table
    """

    # pylint: disable=too-few-public-methods

    column_var_type_map: Dict[StrictStr, DBVarType]
    credentials: Optional[Credentials] = Field(default=None, allow_mutation=False)

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
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Construct additional parameter mappings to input node during node insertion

        Parameters
        ----------
        values: dict[str, Any]
            Dictionary contains parameter name to value mapping for the DatabaseTable object

        Returns
        -------
        dict[str, Any]
        """
        _ = values
        return {}

    @root_validator(pre=True)
    @classmethod
    def _set_graph_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Construct input node & set the graph related parameters based on the given input dictionary

        Parameters
        ----------
        values: dict[str, Any]
            Dictionary contains parameter name to value mapping for the DatabaseTable object

        Returns
        -------
        dict[str, Any]
        """
        credentials = values.get("credentials")
        if credentials is None:
            config = Configurations()
            credentials = config.credentials

        database_source, table_details = values["tabular_source"]
        if isinstance(database_source, dict):
            database_source = ExtendedFeatureStoreModel(**database_source)
        elif isinstance(database_source, FeatureStoreModel):
            database_source = ExtendedFeatureStoreModel(**database_source.dict())
        if isinstance(table_details, dict):
            table_details = TableDetails(**table_details)

        session = database_source.get_session(credentials=credentials)
        table_schema = session.list_table_schema(
            database_name=table_details.database_name,
            schema_name=table_details.schema_name,
            table_name=table_details.table_name,
        )

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": list(table_schema.keys()),
                "dbtable": table_details.dict(),
                "database_source": database_source.dict(),
                **cls._get_other_input_node_parameters(values),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        values["node"] = node
        values["row_index_lineage"] = (node.name,)
        values["column_var_type_map"] = table_schema
        return values

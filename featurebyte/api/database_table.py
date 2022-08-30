"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import Field, root_validator

from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Configurations, Credentials
from featurebyte.core.frame import BaseFrame
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.exception import TableSchemaHasBeenChangedError
from featurebyte.models.feature_store import ColumnInfo, DatabaseTableModel, TableDetails
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class DatabaseTable(DatabaseTableModel, BaseFrame):
    """
    DatabaseTable class to preview table
    """

    # pylint: disable=too-few-public-methods

    credentials: Optional[Credentials] = Field(default=None, allow_mutation=False, exclude=True)
    feature_store: ExtendedFeatureStoreModel = Field(allow_mutation=False, exclude=True)

    class Config:
        """
        Pydantic Config class
        """

        fields = {
            "graph": {"exclude": True},
            "node": {"exclude": True},
            "row_index_lineage": {"exclude": True},
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
    def _set_feature_store_and_graph_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        """
        Construct feature_store, input node & set the graph related parameters based on the given input dictionary

        Parameters
        ----------
        values: dict[str, Any]
            Dictionary contains parameter name to value mapping for the DatabaseTable object

        Returns
        -------
        dict[str, Any]

        Raises
        ------
        TableSchemaHasBeenChangedError
            When table schema has been changed
        """
        credentials = values.get("credentials")
        config = Configurations()
        if credentials is None:
            credentials = config.credentials

        tabular_source = dict(values["tabular_source"])
        table_details = tabular_source["table_details"]
        if "feature_store" not in values:
            # attempt to set feature_store object if it does not exist
            values["feature_store"] = FeatureStore.get_by_id(id=tabular_source["feature_store_id"])

        feature_store = values["feature_store"]
        if isinstance(table_details, dict):
            table_details = TableDetails(**table_details)

        session = feature_store.get_session(credentials=credentials)
        recent_schema = session.list_table_schema(
            database_name=table_details.database_name,
            schema_name=table_details.schema_name,
            table_name=table_details.table_name,
        )

        if "columns_info" in values:
            columns_info = [ColumnInfo(**dict(col)) for col in values["columns_info"]]
            schema = {col.name: col.var_type for col in columns_info}
            if not recent_schema.items() >= schema.items():
                raise TableSchemaHasBeenChangedError("Table schema has been changed.")
        else:
            columns_info = [
                ColumnInfo(name=name, var_type=var_type) for name, var_type in recent_schema.items()
            ]
            values["columns_info"] = columns_info

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": [col.name for col in columns_info],
                "dbtable": table_details.dict(),
                "feature_store": feature_store.dict(include={"type": True, "details": True}),
                **cls._get_other_input_node_parameters(values),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        values["node"] = node
        values["row_index_lineage"] = (node.name,)
        return values

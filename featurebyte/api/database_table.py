"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any

from http import HTTPStatus

from pydantic import Field, root_validator

from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.feature_store import (
    ColumnInfo,
    DatabaseTableModel,
    FeatureStoreModel,
    TableDetails,
)
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph


class DatabaseTable(DatabaseTableModel, BaseFrame):
    """
    DatabaseTable class to preview table
    """

    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)

    class Config:
        """
        Pydantic Config class
        """

        fields = {
            "graph": {"exclude": True},
            "node_name": {"exclude": True},
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
        return {"type": TableDataType.GENERIC}

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
        RecordRetrievalException
            Failed to retrieve table schema
        """
        tabular_source = dict(values["tabular_source"])
        table_details = tabular_source["table_details"]
        if "feature_store" not in values:
            # attempt to set feature_store object if it does not exist
            from featurebyte.api.feature_store import (  # pylint: disable=import-outside-toplevel,cyclic-import
                FeatureStore,
            )

            values["feature_store"] = FeatureStore.get_by_id(id=tabular_source["feature_store_id"])

        feature_store = values["feature_store"]
        if isinstance(table_details, dict):
            table_details = TableDetails(**table_details)

        client = Configurations().get_client()
        response = client.post(
            url=(
                f"/feature_store/column?"
                f"database_name={table_details.database_name}&"
                f"schema_name={table_details.schema_name}&"
                f"table_name={table_details.table_name}"
            ),
            json=feature_store.json_dict(),
        )
        if response.status_code == HTTPStatus.OK:
            column_specs = response.json()
            recent_schema = {
                column_spec["name"]: DBVarType(column_spec["dtype"]) for column_spec in column_specs
            }
        else:
            raise RecordRetrievalException(response)

        if "columns_info" in values:
            columns_info = [ColumnInfo(**dict(col)) for col in values["columns_info"]]
            schema = {col.name: col.dtype for col in columns_info}
            if not recent_schema.items() >= schema.items():
                logger.warning("Table schema has been changed.")
        else:
            columns_info = [
                ColumnInfo(name=name, dtype=var_type) for name, var_type in recent_schema.items()
            ]
            values["columns_info"] = columns_info

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": [col.name for col in columns_info],
                "table_details": table_details.dict(),
                "feature_store_details": feature_store.dict(
                    include={"type": True, "details": True}
                ),
                **cls._get_other_input_node_parameters(values),
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        values["node_name"] = node.name
        values["row_index_lineage"] = (node.name,)
        return values

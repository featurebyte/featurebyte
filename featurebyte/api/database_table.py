"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional, Tuple, Type, cast

from abc import ABC
from http import HTTPStatus

from pydantic import Field, StrictStr, root_validator

from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.table import AllTableDataT, ConstructNodeMixin, GenericTableData
from featurebyte.query_graph.node.schema import FeatureStoreDetails, TableDetails


class AbstractTableDataFrame(BaseFrame, ConstructNodeMixin, FeatureByteBaseModel, ABC):
    """
    AbstractTableDataFrame class represents the table data as a frame (in query graph context).
    """

    node_name: str = Field(default_factory=str)
    row_index_lineage: Tuple[StrictStr, ...] = Field(default_factory=tuple, exclude=True)
    column_lineage_map: Dict[str, Tuple[str, ...]] = Field(default_factory=dict, exclude=True)
    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)
    _table_data_class: ClassVar[Optional[Type[AllTableDataT]]] = None

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

        if "graph" not in values:
            assert cls._table_data_class is not None
            graph = QueryGraph()
            table_data = cast(ConstructNodeMixin, cls._table_data_class(**values))
            input_node = table_data.construct_input_node(
                feature_store_details=FeatureStoreDetails(**feature_store.dict())
            )
            inserted_node = graph.add_node(node=input_node, input_nodes=[])
            values["graph"] = graph
            values["node_name"] = inserted_node.name

        return values

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        node = GlobalQueryGraph().add_node(
            self.construct_input_node(
                feature_store_details=FeatureStoreDetails(**self.feature_store.dict())
            ),
            input_nodes=[],
        )
        self.node_name = node.name
        self.row_index_lineage = (node.name,)
        for col in self.columns:
            self.column_lineage_map[col] = (node.name,)


class DatabaseTable(GenericTableData, AbstractTableDataFrame):
    """
    DatabaseTable class to preview table
    """

    _table_data_class = GenericTableData

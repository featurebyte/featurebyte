"""
DatabaseTable class
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Type, Union

from abc import ABC
from datetime import datetime
from http import HTTPStatus

import pandas as pd
from pydantic import Field, root_validator
from typeguard import typechecked

from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ConstructGraphMixin, FeatureStoreModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.table import AllTableDataT, GenericTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import InputNode
from featurebyte.query_graph.node.schema import TableDetails


class AbstractTableDataFrame(BaseFrame, ConstructGraphMixin, FeatureByteBaseModel, ABC):
    """
    AbstractTableDataFrame class represents the table data as a frame (in query graph context).
    """

    node_name: str = Field(default_factory=str)
    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)
    _table_data_class: ClassVar[Type[AllTableDataT]]

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

        to_validate_schema = values.get("_validate_schema") or "columns_info" not in values
        if to_validate_schema:
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
                    column_spec["name"]: DBVarType(column_spec["dtype"])
                    for column_spec in column_specs
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
                    ColumnInfo(name=name, dtype=var_type)
                    for name, var_type in recent_schema.items()
                ]
                values["columns_info"] = columns_info

        # construct graph & node
        # table node contains data id, this part is to ensure that the id is passed to table node parameters
        table_data_dict = values.copy()
        if "id" in table_data_dict and "_id" not in table_data_dict:
            table_data_dict["_id"] = table_data_dict["id"]

        graph, node = cls.construct_graph_and_node(
            feature_store_details=feature_store.get_feature_store_details(),
            table_data_dict=table_data_dict,
        )
        values["graph"] = graph
        values["node_name"] = node.name
        return values

    @property
    def table_data(self) -> BaseTableData:
        """
        Table data object for the given data model

        Returns
        -------
        BaseTableData
        """
        return self._table_data_class(**self.json_dict())

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        node = self.node
        if kwargs.get("after_cleaning"):
            assert isinstance(node, InputNode)
            graph_node = self.table_data.construct_cleaning_recipe_node(input_node=node)
            if graph_node:
                node = GlobalQueryGraph().add_node(node=graph_node, input_nodes=[self.node])

        pruned_graph, node_name_map = GlobalQueryGraph().prune(target_node=node, aggressive=True)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[node.name])
        return pruned_graph, mapped_node

    @typechecked
    def preview_clean_data_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the table data after applying list of cleaning operations

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        return self._preview_sql(limit=limit, after_cleaning=True)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False, **kwargs: Any) -> pd.DataFrame:
        """
        Preview raw or clean table data

        Parameters
        ----------
        limit: int
            Maximum number of return rows
        after_cleaning: bool
            Whether to apply cleaning operations
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        pd.DataFrame
        """
        return super().preview(limit=limit, after_cleaning=after_cleaning, **kwargs)  # type: ignore[misc]

    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Sample raw or clean table data

        Parameters
        ----------
        size: int
            Maximum number of rows to sample
        seed: int
            Seed to use for random sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from
        after_cleaning: bool
            Whether to apply cleaning operations
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        pd.DataFrame
        """
        return super().sample(  # type: ignore[misc]
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Describe raw or clean table data

        Parameters
        ----------
        size: int
            Maximum number of rows to sample
        seed: int
            Seed to use for random sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from
        after_cleaning: bool
            Whether to apply cleaning operations
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        pd.DataFrame
        """
        return super().describe(  # type: ignore[misc]
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )


class DatabaseTable(GenericTableData, AbstractTableDataFrame):
    """
    DatabaseTable class to preview table
    """

    _table_data_class = GenericTableData

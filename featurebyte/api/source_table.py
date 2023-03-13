"""
SourceTable class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Type, Union

from abc import ABC, abstractmethod
from datetime import datetime
from http import HTTPStatus

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.config import Configurations
from featurebyte.core.frame import BaseFrame
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logger import logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import ConstructGraphMixin, FeatureStoreModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import BaseTableData, TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.model.table import AllTableDataT, SouceTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.schema import TableDetails


class TableDataFrame(BaseFrame):
    """
    TableDataFrame class is a frame encapsulation of the table objects (like event table, item table).
    This class is used to construct the query graph for previewing/sampling underlying table stored at the
    data warehouse. The constructed query graph is stored locally (not loaded into the global query graph).
    """

    table_data: BaseTableData

    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        node = self.node
        if kwargs.get("after_cleaning"):
            assert isinstance(node, InputNode)
            graph_node = self.table_data.construct_cleaning_recipe_node(
                input_node=node, skip_column_names=[]
            )
            if graph_node:
                node = self.graph.add_node(node=graph_node, input_nodes=[self.node])

        pruned_graph, node_name_map = self.graph.prune(target_node=node, aggressive=True)
        mapped_node = pruned_graph.get_node_by_name(node_name_map[node.name])
        return pruned_graph, mapped_node


class AbstractTableData(ConstructGraphMixin, FeatureByteBaseModel, ABC):
    """
    AbstractTableDataFrame class represents the table.
    """

    # class variables
    _table_data_class: ClassVar[Type[AllTableDataT]]

    # pydantic instance variable (public)
    tabular_source: TabularSource = Field(allow_mutation=False)
    feature_store: FeatureStoreModel = Field(allow_mutation=False, exclude=True)

    # pydantic instance variable (internal use)
    internal_columns_info: List[ColumnInfo] = Field(alias="columns_info")

    def __init__(self, **kwargs: Any):
        # Construct feature_store, input node & set the graph related parameters based on the given input dictionary
        values = kwargs
        tabular_source = dict(values["tabular_source"])
        table_details = tabular_source["table_details"]
        if "feature_store" not in values:
            # attempt to set feature_store object if it does not exist in the input
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

        # call pydantic constructor to validate input parameters
        super().__init__(**values)

    @property
    def table_data(self) -> BaseTableData:
        """
        Table table object for the given table model

        Returns
        -------
        BaseTableData
        """
        return self._table_data_class(**self.json_dict())

    @property
    def frame(self) -> TableDataFrame:
        """
        Frame object of this table object. The frame is constructed from a local query graph.

        Returns
        -------
        TableDataFrame
        """
        # Note that the constructed query graph WILL NOT BE INSERTED into the global query graph.
        # Only when the view is constructed, the local query graph (constructed by table object) is then loaded
        # into the global query graph.
        graph, node = self.construct_graph_and_node(
            feature_store_details=self.feature_store.get_feature_store_details(),
            table_data_dict=self.table_data.dict(by_alias=True),
        )
        return TableDataFrame(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            table_data=self.table_data,
            columns_info=self.columns_info,
            graph=graph,
            node_name=node.name,
        )

    @property
    def column_var_type_map(self) -> dict[str, DBVarType]:
        """
        Column name to DB var type mapping

        Returns
        -------
        dict[str, DBVarType]
        """
        return {col.name: col.dtype for col in self.columns_info}

    @property
    def dtypes(self) -> pd.Series:
        """
        Retrieve column table type info

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

    @property
    def columns(self) -> list[str]:
        """
        Columns of the object

        Returns
        -------
        list[str]
        """
        return list(self.column_var_type_map)

    @typechecked
    def preview_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit)

    @typechecked
    def preview_clean_data_sql(self, limit: int = 10) -> str:
        """
        Generate SQL query to preview the table after applying list of cleaning operations

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit, after_cleaning=True)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False, **kwargs: Any) -> pd.DataFrame:
        """
        Preview raw or clean table

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
        return self.frame.preview(limit=limit, after_cleaning=after_cleaning, **kwargs)  # type: ignore[misc]

    @typechecked
    def sample(
        self,
        size: int = 10,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Sample raw or clean table

        Parameters
        ----------
        size: int
            Maximum number of rows to sample
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
        return self.frame.sample(  # type: ignore[misc]
            size=size,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )

    @typechecked
    def describe(
        self,
        size: int = 0,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Describe raw or clean table

        Parameters
        ----------
        size: int
            Maximum number of rows to sample
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
        return self.frame.describe(  # type: ignore[misc]
            size=size,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
            **kwargs,
        )

    @property
    @abstractmethod
    def columns_info(self) -> List[ColumnInfo]:
        """
        List of column information of the dataset. Each column contains column name, column type, entity ID
        associated with the column, semantic ID associated with the column.

        Returns
        -------
        List[ColumnInfo]
        """


class SourceTable(AbstractTableData):
    """
    SourceTable class to preview table
    """

    _table_data_class: ClassVar[Type[AllTableDataT]] = SouceTableData

    @property
    def columns_info(self) -> List[ColumnInfo]:
        return self.internal_columns_info

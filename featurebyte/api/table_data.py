"""
Table data module.
"""
from typing import Any, ClassVar, List, Optional, Tuple, Type, Union, cast

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
from featurebyte.query_graph.model.table import AllTableDataT
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
    int_timestamp_column: Optional[str] = Field(alias="timestamp_column")

    @property
    def timestamp_column(self) -> Optional[str]:
        return self.int_timestamp_column

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
        Table data object of this table. This object contains information used to construct the SQL query.

        Returns
        -------
        BaseTableData
            Table data object used for SQL query construction.
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
            timestamp_column=self.timestamp_column,
        )

    @property
    def column_var_type_map(self) -> dict[str, DBVarType]:
        """
        Column name to DB var type mapping.

        Returns
        -------
        dict[str, DBVarType]
        """
        return {col.name: col.dtype for col in self.columns_info}

    @property
    def dtypes(self) -> pd.Series:
        """
        Retrieve column table type info.

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

    @property
    def columns(self) -> list[str]:
        """
        Returns the list of column names of this table.

        Returns
        -------
        list[str]
            List of column name strings.
        """
        return list(self.column_var_type_map)

    @typechecked
    def preview_sql(self, limit: int = 10, after_cleaning: bool = False) -> str:
        """
        Returns an SQL query for previewing the table raw data.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        str
        """
        return self.frame.preview_sql(limit=limit, after_cleaning=after_cleaning)

    @typechecked
    def preview(self, limit: int = 10, after_cleaning: bool = False) -> pd.DataFrame:
        """
        Retrieve a preview of the table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        Preview 3 rows of the table.
        >>> catalog.get_table("GROCERYPRODUCT").preview(3)
                             GroceryProductGuid ProductGroup
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f      Glaçons
        1  116c9284-2c41-446e-8eee-33901e0acdef      Glaçons
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375      Glaçons

        See Also
        --------
        - [Table.sample](/reference/featurebyte.api.base_table.TableApiObject.sample/):
          Retrieve a sample of a table.
        - [Table.describe](/reference/featurebyte.api.base_table.TableApiObject.describe/):
          Retrieve a summary of a table.
        """
        return self.frame.preview(limit=limit, after_cleaning=after_cleaning)

    @typechecked
    def shape(self, after_cleaning: bool = False) -> Tuple[int, int]:
        """
        Return the shape of the view / column.

        Parameters
        ----------
        after_cleaning: bool
            Whether to get the shape of the table after cleaning

        Returns
        -------
        Tuple[int, int]

        Examples
        --------
        Get the shape of a table.
        >>> catalog.get_table("INVOICEITEMS").shape()
        (300450, 8)
        """
        return cast(Tuple[int, int], self.frame.shape(after_cleaning=after_cleaning))

    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the table based on a specified time range,
        size, and seed for sampling control. By default, the materialization process occurs before any cleaning
        operations that were defined at the column level.

        It's important to keep in mind that views originating from tables in a Snowflake data warehouse do not allow
        the use of a seed.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.

        Examples
        --------
        Sample 3 rows from the table.
        >>> catalog.get_table("GROCERYPRODUCT").sample(3)
                             GroceryProductGuid ProductGroup
        0  e890c5cb-689b-4caf-8e49-6b97bb9420c0       Épices
        1  5720e4df-2996-4443-a1bc-3d896bf98140         Chat
        2  96fc4d80-8cb0-4f1b-af01-e71ad7e7104a        Pains

        See Also
        --------
        - [Table.preview](/reference/featurebyte.api.base_table.TableApiObject.preview/):
          Retrieve a preview of a table.
        - [Table.describe](/reference/featurebyte.api.base_table.TableApiObject.describe/):
          Retrieve a summary of a table.
        """
        return self.frame.sample(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )

    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        after_cleaning: bool = False,
    ) -> pd.DataFrame:
        """
        Retrieve a summary of the contents in the table.
        This includes columns names, column types, missing and unique counts, and other statistics.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        after_cleaning: bool
            Whether to apply cleaning operations.

        Returns
        -------
        pd.DataFrame
            Summary of the table.

        Examples
        --------
        Get a summary of a view.
        >>> catalog.get_table("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                         1                1319

        See Also
        --------
        - [Table.preview](/reference/featurebyte.api.base_table.TableApiObject.preview/):
          Retrieve a preview of a table.
        - [Table.sample](/reference/featurebyte.api.base_table.TableApiObject.sample/):
          Retrieve a sample of a table.
        """
        return self.frame.describe(
            size=size,
            seed=seed,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            after_cleaning=after_cleaning,
        )

    @property
    @abstractmethod
    def timestamp_column(self) -> Optional[str]:
        """
        Name of the timestamp column.

        Returns
        -------
        Optional[str]
        """

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

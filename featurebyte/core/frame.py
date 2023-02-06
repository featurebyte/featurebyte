"""
Frame class
"""
from __future__ import annotations

from typing import Any, List, TypeVar, Union

import pandas as pd
from pydantic import Field
from typeguard import typechecked

from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import GetAttrMixin, OpsMixin, SampleMixin
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo

InputTypeT = TypeVar("InputTypeT", bound=Series)


class BaseFrame(QueryObject, SampleMixin):
    """
    BaseFrame class

    Parameters
    ----------
    columns_info: List[ColumnInfo]
        List of column specifications that are contained in this frame.
    """

    columns_info: List[ColumnInfo] = Field(description="List of columns specifications")

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
        Retrieve column data type info

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


class Frame(BaseFrame, OpsMixin, GetAttrMixin):
    """
    Implement operations to manipulate database table
    """

    _series_class = Series

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {}

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _check_any_missing_column(self, item: str | list[str] | Series) -> None:
        """
        Check whether there is any unknown column from the specified item (single column or list of columns)

        Parameters
        ----------
        item: str | list[str]
            input column(s)

        Raises
        ------
        KeyError
            if the specified column does not exist
        """
        if isinstance(item, str):
            if item not in self.column_var_type_map:
                raise KeyError(f"Column {item} not found!")
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            not_found_columns = [elem for elem in item if elem not in self.column_var_type_map]
            if not_found_columns:
                raise KeyError(f"Columns {not_found_columns} not found!")

    @typechecked
    def __getitem__(self, item: Union[str, List[str], Series]) -> Union[InputTypeT, Series, Frame]:
        """
        Extract column or perform row filtering on the table. When the item has a `str` or `list[str]` type,
        column(s) projection is expected. When the item has a boolean `Series` type, row filtering operation
        is expected.

        Parameters
        ----------
        item: Union[str, List[str], Series]
            input item used to perform column(s) projection or row filtering

        Returns
        -------
        Series or Frame
        """
        self._check_any_missing_column(item)
        if isinstance(item, str):
            # When single column projection happens, the last node of the Series lineage
            # (from the operation structure) is used rather than the DataFrame's last node (`self.node`).
            # This is to prevent adding redundant project node to the graph when the value of the column does
            # not change. Consider the following case if `self.node` is used:
            # >>> df["c"] = df["b"]
            # >>> b = df["b"]
            # >>> dict(df.graph.edges)
            # {
            #     "input_1": ["project_1", "assign_1"],
            #     "project_1": ["assign_1"],
            #     "assign_1": ["project_2"]
            # }
            # Current implementation uses the last node of each lineage, it results in a simpler graph:
            # >>> dict(df.graph.edges)
            # {
            #     "input_1": ["project_1", "assign_1"],
            #     "project_1": ["assign_1"],
            # }
            op_struct = self.graph.extract_operation_structure(node=self.node)
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.graph.get_node_by_name(op_struct.get_column_node_name(item))],
            )
            output = self._series_class(
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                node_name=node.name,
                name=item,
                dtype=self.column_var_type_map[item],
                **self._getitem_series_params,
            )
            output.set_parent(self)
            return output
        if isinstance(item, list):
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": item},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            return type(self)(
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                columns_info=[col for col in self.columns_info if col.name in item],
                node_name=node.name,
                **self._getitem_frame_params,
            )
        # item must be Series type
        node = self._add_filter_operation(
            item=self, mask=item, node_output_type=NodeOutputType.FRAME
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=self.columns_info,
            node_name=node.name,
            **self._getitem_frame_params,
        )

    @typechecked
    def __setitem__(self, key: str, value: Union[int, float, str, bool, InputTypeT]) -> None:
        """
        Assign a scalar value or Series object of the same `dtype` to the `Frame` object

        Parameters
        ----------
        key: str
            column name to store the item
        value: Union[int, float, str, bool, Series]
            value to be assigned to the column

        Raises
        ------
        ValueError
            when the row indices between the Frame object & value object are not aligned
        """
        if isinstance(value, Series):
            if self.row_index_lineage != value.row_index_lineage:
                raise ValueError(f"Row indices between '{self}' and '{value}' are not aligned!")
            node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node, value.node],
            )
            self.columns_info.append(ColumnInfo(name=key, dtype=value.dtype))
        else:
            node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"value": value, "name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            self.columns_info.append(
                ColumnInfo(name=key, dtype=self.pytype_dbtype_map[type(value)])
            )

        # update node_name
        self.node_name = node.name

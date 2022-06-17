"""
Frame class
"""
from __future__ import annotations

import copy
from collections import OrderedDict

import pandas as pd

from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import OpsMixin
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, Node
from featurebyte.session.base import BaseSession


class Frame(QueryObject, OpsMixin):
    """
    Implement operations to manipulate database table
    """

    series_class = Series

    def __init__(
        self,
        node: Node,
        column_var_type_map: dict[str, DBVarType],
        column_lineage_map: dict[str, tuple[str, ...]],
        row_index_lineage: tuple[str, ...],
        session: BaseSession | None = None,
    ):
        # pylint: disable=R0801 (duplicate-code)
        super().__init__(
            graph=GlobalQueryGraph(),
            node=node,
            row_index_lineage=row_index_lineage,
            session=session,
        )
        self.column_var_type_map = OrderedDict(column_var_type_map)
        self.column_lineage_map = column_lineage_map

    @property
    def columns(self) -> list[str]:
        """
        Columns of the object

        Returns
        -------
        list
        """
        return list(self.column_var_type_map)

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

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        """
        Extract column or perform row filtering on the table

        When the item has a `str` or `list[str]` type, column(s) projection is expected. When single column
        projection happens, the last node of the Series lineage (`self.column_lineage_map[item][-1]`) is used
        rather than the DataFrame's last node (`self.node`). This is to prevent adding redundant project node
        to the graph when the value of the column does not change. Consider the following case if `self.node`
        is used:

        >>> df["c"] = df["b"]
        >>> b = df["b"]
        >>> dict(df.graph.edges)
        {
            "input_1": ["project_1", "assign_1"],
            "project_1": ["assign_1"],
            "assign_1": ["project_2"]
        }

        Current implementation uses the last node of each lineage, it results in a simpler graph:

        >>> dict(df.graph.edges)
        {
            "input_1": ["project_1", "assign_1"],
            "project_1": ["assign_1"],
        }

        When the item has a boolean `Series` type, row filtering operation is expected.

        Parameters
        ----------
        item: str | list[str] | Series
            input item used to perform column(s) projection or row filtering

        Returns
        -------
        Series | Frame
            output of the operation

        Raises
        ------
        TypeError
            When the item type does not support
        """
        self._check_any_missing_column(item)

        if isinstance(item, str):
            # when doing projection, use the last updated node of the column rather than using
            # the last updated dataframe node to prevent adding redundant project node to the graph.
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.graph.get_node_by_name(self.column_lineage_map[item][-1])],
            )
            return self.series_class(
                node=node,
                name=item,
                var_type=self.column_var_type_map[item],
                lineage=self._append_to_lineage(self.column_lineage_map[item], node.name),
                row_index_lineage=self.row_index_lineage,
                session=self.session,
            )
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": item},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            column_var_type_map = {
                col: var_type for col, var_type in self.column_var_type_map.items() if col in item
            }
            column_lineage_map = {}
            for col in item:
                column_lineage_map[col] = self._append_to_lineage(
                    self.column_lineage_map[col], node.name
                )
            return type(self)(
                node=node,
                column_var_type_map=column_var_type_map,
                column_lineage_map=column_lineage_map,
                row_index_lineage=self.row_index_lineage,
                session=self.session,
            )
        if isinstance(item, Series):
            node = self._add_filter_operation(
                item=self, mask=item, node_output_type=NodeOutputType.FRAME
            )
            column_lineage_map = {}
            for col, lineage in self.column_lineage_map.items():
                column_lineage_map[col] = self._append_to_lineage(lineage, node.name)
            return type(self)(
                node=node,
                column_var_type_map=copy.deepcopy(self.column_var_type_map),
                column_lineage_map=column_lineage_map,
                row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
                session=self.session,
            )
        raise TypeError(f"Frame indexing with value '{item}' not supported!")

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        """
        Assign a scalar value or Series object of the same `var_type` to the `Frame` object

        Parameters
        ----------
        key: str
            column name to store the item
        value: int | float | str | bool | Series
            value to be assigned to the column

        Raises
        ------
        ValueError
            when the row indices between the Frame object & value object are not aligned
        TypeError
            when the key type & value type combination is not supported
        """
        if isinstance(key, str) and self.is_supported_scalar_pytype(value):
            self.node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"value": value, "name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            self.column_var_type_map[key] = self.pytype_dbtype_map[type(value)]
            self.column_lineage_map[key] = self._append_to_lineage(
                self.column_lineage_map.get(key, tuple()), self.node.name
            )
        elif isinstance(key, str) and isinstance(value, Series):
            if self.row_index_lineage != value.row_index_lineage:
                raise ValueError(f"Row indices between '{self}' and '{value}' are not aligned!")
            self.node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node, value.node],
            )
            self.column_var_type_map[key] = value.var_type
            self.column_lineage_map[key] = self._append_to_lineage(
                self.column_lineage_map.get(key, tuple()), self.node.name
            )
        else:
            raise TypeError(f"Setting key '{key}' with value '{value}' not supported!")

    def preview(self, limit: int = 10) -> pd.DataFrame | None:
        """
        Preview transformed table/column partial output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame | None
        """
        return self._preview(columns=self.columns, limit=limit)

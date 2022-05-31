"""
Frame class
"""
from __future__ import annotations

import copy

from featurebyte.core.operation import OpsMixin
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


class Frame(OpsMixin):
    """
    Implement operations to manipulate database table
    """

    def __init__(
        self,
        node: Node,
        column_var_type_map: dict[str, DBVarType],
        column_lineage_map: dict[str, tuple[str, ...]],
        row_index_lineage: tuple[str, ...],
    ):
        self.graph = QueryGraph()
        self.node = node
        self.column_var_type_map = column_var_type_map
        self.column_lineage_map = column_lineage_map
        self.row_index_lineage = row_index_lineage

    def __repr__(self) -> str:
        return f"Frame(node.name={self.node.name})"

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        """
        Extract column or perform row filtering on the table.

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
        Series | Frame:
            output of the operation

        Raises
        ------
        KeyError
            When the selected column does not exist
        TypeError
            When the item type does not support
        """
        if isinstance(item, str):
            if item not in self.column_var_type_map:
                raise KeyError(f"Column {item} not found!")
            # when doing projection, use the last updated node of the column rather than using
            # the last updated dataframe node to prevent adding redundant project node to the graph.
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.graph.get_node_by_name(self.column_lineage_map[item][-1])],
            )
            return Series(
                node=node,
                name=item,
                var_type=self.column_var_type_map[item],
                lineage=self._append_to_lineage(self.column_lineage_map[item], node.name),
                row_index_lineage=self.row_index_lineage,
            )
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            not_found_columns = [elem for elem in item if elem not in self.column_var_type_map]
            if not_found_columns:
                raise KeyError(f"Columns {not_found_columns} not found!")
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
            return Frame(
                node=node,
                column_var_type_map=column_var_type_map,
                column_lineage_map=column_lineage_map,
                row_index_lineage=self.row_index_lineage,
            )
        if isinstance(item, Series):
            node = self._add_filter_operation(
                item=self, mask=item, node_output_type=NodeOutputType.FRAME
            )
            column_lineage_map = {}
            for col, lineage in self.column_lineage_map.items():
                column_lineage_map[col] = self._append_to_lineage(lineage, node.name)
            return Frame(
                node=node,
                column_var_type_map=copy.deepcopy(self.column_var_type_map),
                column_lineage_map=column_lineage_map,
                row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
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

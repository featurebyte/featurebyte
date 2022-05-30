"""
DataFrame class
"""
from __future__ import annotations

import copy

from featurebyte.enum import DBVarType
from featurebyte.pandas.operation import OpsMixin
from featurebyte.pandas.series import Series
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


class DataFrame(OpsMixin):
    """
    Implement Pandas DataFrame like operations to manipulate database table
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
        return f"DataFrame(node.name={self.node.name})"

    def __getitem__(self, item: str | list[str] | Series) -> Series | DataFrame:
        if isinstance(item, str):
            if item not in self.column_var_type_map:
                raise KeyError(f"Column {item} not found!")
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
            return DataFrame(
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
            return DataFrame(
                node=node,
                column_var_type_map=copy.deepcopy(self.column_var_type_map),
                column_lineage_map=column_lineage_map,
                row_index_lineage=self._append_to_lineage(self.row_index_lineage, node.name),
            )
        raise TypeError(f"DataFrame indexing with value '{item}' not supported!")

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if isinstance(key, str) and isinstance(value, (int, float, str, bool)):
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
            raise TypeError(f"Key '{key}' not supported!")

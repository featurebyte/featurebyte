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
        self, node: Node, column_var_type_map: dict[str, DBVarType], row_index_lineage: list[str]
    ):
        self.graph = QueryGraph()
        self.node = node
        self.column_var_type_map = column_var_type_map
        self.row_index_lineage = tuple(row_index_lineage)

    def __getitem__(self, item: str | list[str] | Series) -> Series | DataFrame:
        lineage = list(self.row_index_lineage)
        if isinstance(item, str):
            if item not in self.column_var_type_map:
                raise KeyError(f"Column {item} not found!")
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node],
            )
            return Series(
                node=node,
                name=item,
                var_type=self.column_var_type_map[item],
                row_index_lineage=lineage,
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
            return DataFrame(
                node=node, column_var_type_map=column_var_type_map, row_index_lineage=lineage
            )
        if isinstance(item, Series):
            node = self._add_filter_operation(
                item=self, mask=item, node_output_type=NodeOutputType.FRAME
            )
            lineage.append(node.name)
            return DataFrame(
                node=node,
                column_var_type_map=copy.deepcopy(self.column_var_type_map),
                row_index_lineage=lineage,
            )
        raise TypeError(f"Type {type(item)} not supported!")

    @classmethod
    def _db_var_type_from(cls, py_type: type[str | int | float | bool]) -> DBVarType:
        type_map = {
            str: DBVarType.VARCHAR,
            int: DBVarType.INT,
            float: DBVarType.FLOAT,
            bool: DBVarType.BOOL,
        }
        return type_map[py_type]

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if isinstance(key, str) and isinstance(value, (int, float, str, bool)):
            self.node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"value": value, "name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            self.column_var_type_map[key] = self._db_var_type_from(type(value))
        elif isinstance(key, str) and isinstance(value, Series):
            if self.row_index_lineage != value.row_index_lineage:
                raise ValueError("Row index not aligned!")
            self.node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node, value.node],
            )
            self.column_var_type_map[key] = value.var_type
        else:
            raise TypeError(f"Key type {type(key)} with value type {type(value)} not supported!")

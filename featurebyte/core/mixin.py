"""
OpsMixin class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from featurebyte.core.protocol import WithQueryGraphProtocol
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter
from featurebyte.session.base import BaseSession

if TYPE_CHECKING:
    from featurebyte.core.frame import Frame
    from featurebyte.core.series import Series


class OpsMixin:
    """
    OpsMixin contains common properties & operations shared between Frame & Series
    """

    @property
    def pytype_dbtype_map(self) -> dict[Any, Any]:
        """
        Supported python builtin scalar type to database type mapping

        Returns
        -------
        dict
            mapping from supported builtin type to DB type
        """
        return {
            bool: DBVarType.BOOL,
            int: DBVarType.INT,
            float: DBVarType.FLOAT,
            str: DBVarType.VARCHAR,
        }

    def is_supported_scalar_pytype(self, item: Any) -> bool:
        """
        Check whether the input item is from the supported scalar types

        Parameters
        ----------
        item: Any
            input item

        Returns
        -------
        bool
            whether the specified item is from the supported scalar types
        """
        return isinstance(item, tuple(self.pytype_dbtype_map))

    @staticmethod
    def _add_filter_operation(
        item: Frame | Series, mask: Series, node_output_type: NodeOutputType
    ) -> Node:
        """
        Add filter node into the graph & return the node

        Parameters
        ----------
        item: Frame | Series
            object to be filtered
        mask: Series
            mask used to filter the item object
        node_output_type: NodeOutputType
            note output type

        Returns
        -------
        Node
            Filter node

        Raises
        ------
        TypeError
            if mask Series is not boolean type
        ValueError
            if the row index between item object & mask are not aligned
        """
        if mask.var_type != DBVarType.BOOL:
            raise TypeError("Only boolean Series filtering is supported!")
        if item.row_index_lineage != mask.row_index_lineage:
            raise ValueError(f"Row indices between '{item}' and '{mask}' are not aligned!")

        node = QueryGraph().add_operation(
            node_type=NodeType.FILTER,
            node_params={},
            node_output_type=node_output_type,
            input_nodes=[item.node, mask.node],
        )
        return node

    @staticmethod
    def _append_to_lineage(lineage: tuple[str, ...], node_name: str) -> tuple[str, ...]:
        """
        Add operation node name to the (row-index) lineage (list of node names)

        Parameters
        ----------
        lineage: tuple[str, ...]
            tuple of node names to represent the feature/row-index lineage
        node_name: str
            operation node name

        Returns
        -------
        updated_lineage: tuple[str, ...]
            updated lineage after adding the new operation name

        """
        output = list(lineage)
        output.append(node_name)
        return tuple(output)


class WithProtectedColumnsMixin(WithQueryGraphProtocol):
    """
    EventSourceFeatureOpsMixin contains common properties & methods shared between EventSource, Feature &
    FeatureList classes
    """

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """

    @property
    def protected_columns(self) -> set[str]:
        """
        Special columns set where values of these columns should not be overridden

        Returns
        -------
        set[str]
        """
        columns = []
        for attr in self.protected_attributes:
            attr_val = getattr(self, attr)
            if attr_val:
                columns.append(attr_val)
        return set(columns)

    @property
    def inception_node(self: WithQueryGraphProtocol) -> Node:
        """
        Input node where the event source is introduced to the query graph

        Returns
        -------
        Node
        """
        graph = QueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])


class WithProtectedColumnsFrameMixin(WithProtectedColumnsMixin):
    """
    WithProtectedColumnsFrameMixin contains common properties & methods shared between Frame child classes with
    protected columns
    """

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Not allow to override special column '{key}'!")
        super().__setitem__(key, value)


class PreviewableMixin(WithQueryGraphProtocol):
    """
    PreviewableMixin provide methods to preview transformed table/column partial output
    """

    session: BaseSession | None

    def preview(self) -> pd.DataFrame | None:
        """
        Preview transformed table/column partial output

        Returns
        -------
        pd.DataFrame | None
        """
        sql_query = GraphInterpreter(self.graph).construct_preview_sql(self.node.name)
        if self.session:
            return self.session.execute_query(sql_query)
        return None

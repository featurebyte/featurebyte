"""
OpsMixin class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, Node

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

        node = GlobalQueryGraph().add_operation(
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


class ParentMixin(BaseModel):
    """
    ParentMixin stores the parent object of the current object
    """

    _parent: Any = PrivateAttr(default=None)

    @property
    def parent(self) -> Any:
        """
        Parent Frame object of the current series

        Returns
        -------
        Any
        """
        return self._parent

    def set_parent(self, parent: Any) -> None:
        """
        Set parent of the current object

        Parameters
        ----------
        parent: Any
            Parent which current series belongs to
        """
        self._parent = parent


class AutoCompletionMixin:
    """
    AutoCompletionMixin contains methods to support tab completion for getattr & getitem
    """

    # pylint: disable=too-few-public-methods

    def __dir__(self) -> Iterable[str]:
        attrs = set(super().__dir__())
        return attrs.union(getattr(self, "column_var_type_map", {}).keys())

    def _ipython_key_completions_(self) -> set[str]:
        return set(getattr(self, "column_var_type_map", {}).keys())

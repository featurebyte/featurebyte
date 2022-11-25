"""
Mixin classes used by core objects
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, Protocol

from pydantic import BaseModel, PrivateAttr, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.node import Node

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
        if mask.dtype != DBVarType.BOOL:
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


class HasColumnVarTypeMap(Protocol):
    """
    Class with column_var_type_map attribute / property
    """

    column_var_type_map: Dict[StrictStr, DBVarType]


class GetAttrMixin:
    """
    GetAttrMixin contains some helper methods to access column in a frame like object
    """

    def __dir__(self: HasColumnVarTypeMap) -> Iterable[str]:
        # provide column name lookup and completion for __getattr__
        attrs = set(object.__dir__(self))
        attrs.difference_update(dir(BaseModel))
        return attrs.union(self.column_var_type_map)

    def _ipython_key_completions_(self: HasColumnVarTypeMap) -> set[str]:
        # provide column name lookup and completion for __getitem__
        return set(self.column_var_type_map)

    def __getattr__(self, item: str) -> Any:
        try:
            return object.__getattribute__(self, item)
        except AttributeError as exc:
            if item in self.column_var_type_map:
                return self.__getitem__(item)
            raise exc

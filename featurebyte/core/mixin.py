"""
Mixin classes used by core objects
"""

from __future__ import annotations

import time
from abc import abstractmethod
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Protocol

import pandas as pd
from pydantic import BaseModel, PrivateAttr, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node

if TYPE_CHECKING:
    from featurebyte.core.frame import FrozenFrame
    from featurebyte.core.series import FrozenSeries


logger = get_logger(__name__)


def perf_logging(func: Any) -> Any:
    """
    Decorator to log function execution time.

    Parameters
    ----------
    func: Any
        Function to decorate

    Returns
    -------
    Any
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.debug(f"Function {func.__name__} took {elapsed} seconds")
        return result

    return wrapper


class OpsMixin:
    """
    OpsMixin contains common properties & operations shared between Frame & Series
    """

    @property
    def pytype_dbtype_map(self) -> dict[Any, DBVarType]:
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
            pd.Timestamp: DBVarType.TIMESTAMP,
        }

    @staticmethod
    def _add_filter_operation(
        item: FrozenFrame | FrozenSeries, mask: FrozenSeries, node_output_type: NodeOutputType
    ) -> Node:
        """
        Add filter node into the graph & return the node

        Parameters
        ----------
        item: FrozenFrame | FrozenSeries
            object to be filtered
        mask: FrozenSeries
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


class HasExtractPrunedGraphAndNode(Protocol):
    """
    Class with extract_pruned_graph_and_node attribute / property
    """

    feature_store: FeatureStoreModel

    @abstractmethod
    def extract_pruned_graph_and_node(self, **kwargs: Any) -> tuple[QueryGraphModel, Node]:
        """
        Extract pruned graph & node from the global query graph

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters

        Raises
        ------
        NotImplementedError
            Method not implemented
        """

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column to be used for datetime filtering during sampling

        Returns
        -------
        Optional[str]
        """
        return None

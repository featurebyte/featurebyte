"""
Mixin classes used by core objects
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Protocol, Union

from abc import abstractmethod
from datetime import datetime
from http import HTTPStatus

import pandas as pd
from pydantic import BaseModel, PrivateAttr, StrictStr
from typeguard import typechecked

from featurebyte.common.utils import validate_datetime_input
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.schema.feature_store import FeatureStorePreview, FeatureStoreSample

if TYPE_CHECKING:
    from featurebyte.core.frame import Frame
    from featurebyte.core.series import Series


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


class HasExtractPrunedGraphAndNode(Protocol):
    """
    Class with extract_pruned_graph_and_node attribute / property
    """

    feature_store: FeatureStoreModel

    @abstractmethod
    def extract_pruned_graph_and_node(self) -> tuple[QueryGraph, Node]:
        """
        Extract pruned graph & node from the global query graph

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


class SampleMixin:
    """
    Supports preview and sample functions
    """

    @typechecked
    def preview(self: HasExtractPrunedGraphAndNode, limit: int = 10) -> pd.DataFrame:
        """
        Preview transformed table/column partial output

        Parameters
        ----------
        limit: int
            maximum number of return rows

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        RecordRetrievalException
            Preview request failed
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = FeatureStorePreview(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node_name=mapped_node.name,
        )
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/preview?limit={limit}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return pd.read_json(response.json(), orient="table", convert_dates=False)

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column to be used for datetime filtering during sampling

        Returns
        -------
        Optional[str]
        """
        return None

    @typechecked
    def sample(
        self: HasExtractPrunedGraphAndNode,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
    ) -> pd.DataFrame:
        """
        Sample transformed table/column

        Parameters
        ----------
        size: int
            Maximum number of rows to sample
        seed: int
            Seed to use for random sampling
        from_timestamp: Optional[datetime]
            Start of date range to sample from
        to_timestamp: Optional[datetime]
            End of date range to sample from

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        RecordRetrievalException
            Preview request failed
        """
        from_timestamp = validate_datetime_input(from_timestamp) if from_timestamp else None
        to_timestamp = validate_datetime_input(to_timestamp) if to_timestamp else None

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = FeatureStoreSample(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node_name=mapped_node.name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=self.timestamp_column,
        )
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/sample?size={size}&seed={seed}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return pd.read_json(response.json(), orient="table", convert_dates=False)

"""
Mixin classes used by core objects
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Protocol, Tuple, Union

import time
from abc import abstractmethod
from datetime import datetime
from functools import wraps
from http import HTTPStatus

import pandas as pd
from pydantic import BaseModel, PrivateAttr, StrictStr
from typeguard import typechecked

from featurebyte.common.utils import dataframe_from_json, validate_datetime_input
from featurebyte.config import Configurations
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)

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


class SampleMixin:
    """
    Supports preview and sample functions
    """

    @perf_logging
    @typechecked
    def preview(self: HasExtractPrunedGraphAndNode, limit: int = 10, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve a preview of the view / column.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Preview rows of the data.

        Raises
        ------
        RecordRetrievalException
            Preview request failed.

        Examples
        --------
        Preview 3 rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").preview(3)
                             GroceryProductGuid ProductGroup
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f      Glaçons
        1  116c9284-2c41-446e-8eee-33901e0acdef      Glaçons
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375      Glaçons

        Preview 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["GroceryProductGuid"].preview(3)
                             GroceryProductGuid
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f
        1  116c9284-2c41-446e-8eee-33901e0acdef
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375

        See Also
        --------
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        - [View.describe](/reference/featurebyte.api.view.View.describe/):
          Retrieve a summary of a view.
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
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
        return dataframe_from_json(response.json())

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column to be used for datetime filtering during sampling

        Returns
        -------
        Optional[str]
        """
        return None

    def _get_sample_payload(
        self: HasExtractPrunedGraphAndNode,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> FeatureStoreSample:
        # construct sample payload
        from_timestamp = validate_datetime_input(from_timestamp) if from_timestamp else None
        to_timestamp = validate_datetime_input(to_timestamp) if to_timestamp else None

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
        return FeatureStoreSample(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node_name=mapped_node.name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=self.timestamp_column,
        )

    @perf_logging
    @typechecked
    def shape(self: HasExtractPrunedGraphAndNode, **kwargs: Any) -> Tuple[int, int]:
        """
        Return the shape of the view / column.

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        Tuple[int, int]

        Raises
        ------
        RecordRetrievalException
            Shape request failed.

        Examples
        --------
        Get the shape of a view.
        >>> catalog.get_view("INVOICEITEMS").shape()
        (300450, 10)
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
        payload = FeatureStorePreview(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node_name=mapped_node.name,
        )
        client = Configurations().get_client()
        response = client.post(url="/feature_store/shape", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        shape = FeatureStoreShape(**response.json())
        return shape.num_rows, shape.num_cols

    @perf_logging
    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a random sample of the view / column.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Sampled rows of the data.

        Raises
        ------
        RecordRetrievalException
            Sample request failed.

        Examples
        --------
        Sample rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").sample(3)
                             GroceryProductGuid ProductGroup
        0  e890c5cb-689b-4caf-8e49-6b97bb9420c0       Épices
        1  5720e4df-2996-4443-a1bc-3d896bf98140         Chat
        2  96fc4d80-8cb0-4f1b-af01-e71ad7e7104a        Pains

        Sample 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].sample(3)
          ProductGroup
        0       Épices
        1         Chat
        2        Pains

        See Also
        --------
        - [View.preview](/reference/featurebyte.api.view.View.preview/):
          Retrieve a preview of a view.
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        """
        payload = self._get_sample_payload(from_timestamp, to_timestamp, **kwargs)  # type: ignore[misc]
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/sample?size={size}&seed={seed}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return dataframe_from_json(response.json())

    @perf_logging
    @typechecked
    def describe(
        self: HasExtractPrunedGraphAndNode,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a summary of the view / column.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Summary of the view.

        Raises
        ------
        RecordRetrievalException
            Describe request failed.

        Examples
        --------
        Get summary of a view.
        >>> catalog.get_view("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                       1.0              1319.0

        Get summary of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].describe()
                        ProductGroup
        dtype                VARCHAR
        unique                    87
        %missing                 0.0
        %empty                     0
        entropy              4.13031
        top       Chips et Tortillas
        freq                  1319.0

        See Also
        --------
        - [View.preview](/reference/featurebyte.api.view.View.preview/):
          Retrieve a preview of a view.
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        """
        from_timestamp = validate_datetime_input(from_timestamp) if from_timestamp else None
        to_timestamp = validate_datetime_input(to_timestamp) if to_timestamp else None

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
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
            url=f"/feature_store/description?size={size}&seed={seed}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return dataframe_from_json(response.json())

    @typechecked
    def preview_sql(self: HasExtractPrunedGraphAndNode, limit: int = 10, **kwargs: Any) -> str:
        """
        Generate SQL query to preview the transformation output

        Parameters
        ----------
        limit: int
            maximum number of return rows
        **kwargs: Any
            Additional keyword parameters

        Returns
        -------
        str
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)
        return GraphInterpreter(
            pruned_graph, source_type=self.feature_store.type
        ).construct_preview_sql(node_name=mapped_node.name, num_rows=limit)[0]

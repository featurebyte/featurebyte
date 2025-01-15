"""
Frame class
"""

from __future__ import annotations

import json
from typing import Any, ClassVar, List, Tuple, TypeVar, Union

import pandas as pd
from pydantic import Field, field_validator
from typeguard import typechecked

from featurebyte.core.generic import QueryObject
from featurebyte.core.mixin import GetAttrMixin, OpsMixin
from featurebyte.core.series import FrozenSeries, Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.validator import construct_unique_name_validator


class BaseFrame(QueryObject):
    """
    BaseFrame class

    Parameters
    ----------
    columns_info: List[ColumnInfo]
        List of column specifications that are contained in this frame.
    """

    columns_info: List[ColumnInfo] = Field(description="List of columns specifications")

    # pydantic validator
    _validate_column_names = field_validator("columns_info", mode="after")(
        construct_unique_name_validator(field="name")
    )

    @property
    def column_var_type_map(self) -> dict[str, DBVarType]:
        """
        Column name to DB var type mapping

        Returns
        -------
        dict[str, DBVarType]
        """
        return {col.name: col.dtype for col in self.columns_info}

    @property
    def column_dtype_info_map(self) -> dict[str, DBVarTypeInfo]:
        """
        Column name to column dtype info mapping

        Returns
        -------
        dict[str, dict[str, Any]]
        """
        return {col.name: col.dtype_info for col in self.columns_info}

    @property
    def dtypes(self) -> pd.Series:
        """
        Returns a Series with the data type of each column in the view.

        Returns
        -------
        pd.Series
        """
        return pd.Series(self.column_var_type_map)

    @property
    def columns(self) -> list[str]:
        """
        Columns of the object

        Returns
        -------
        list[str]
        """
        return [col.name for col in self.columns_info]


FrozenFrameT = TypeVar("FrozenFrameT", bound="FrozenFrame")


class FrozenFrame(GetAttrMixin, BaseFrame, OpsMixin):
    """
    FrozenFrame class used for representing a table in the query graph with the ability to perform
    column(s) subsetting and row filtering. This class is immutable as it does not support
    in-place modification of the table in the query graph.
    """

    # class variables
    _series_class: ClassVar[Any] = FrozenSeries

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {}

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {}

    def _check_any_missing_column(self, item: str | list[str] | FrozenSeries) -> None:
        """
        Check whether there is any unknown column from the specified item (single column or list of columns)

        Parameters
        ----------
        item: str | list[str] | FrozenSeries
            input column(s)

        Raises
        ------
        KeyError
            if the specified column does not exist
        """
        if isinstance(item, str):
            if item not in self.column_var_type_map:
                raise KeyError(f"Column {json.dumps(item)} not found!")
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            not_found_columns = [elem for elem in item if elem not in self.column_var_type_map]
            if not_found_columns:
                raise KeyError(f"Columns {json.dumps(not_found_columns)} not found!")

    @typechecked
    def __getitem__(
        self, item: Union[str, List[str], FrozenSeries]
    ) -> Union[FrozenSeries, FrozenFrame]:
        """
        Extract column or perform row filtering on the table. When the item has a `str` or `list[str]` type,
        column(s) projection is expected. When the item has a boolean `Series` type, row filtering operation
        is expected.

        Parameters
        ----------
        item: Union[str, List[str], FrozenSeries]
            input item used to perform column(s) projection or row filtering

        Returns
        -------
        FrozenSeries or FrozenFrame
        """
        self._check_any_missing_column(item)
        if isinstance(item, str):
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [item]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[self.node],
            )
            output = self._series_class(
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                node_name=node.name,
                name=item,
                dtype=self.column_var_type_map[item],
                **self._getitem_series_params,
            )
            output.set_parent(self)
            return output  # type: ignore

        if isinstance(item, list):
            node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": item},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            return type(self)(
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                columns_info=[col for col in self.columns_info if col.name in item],
                node_name=node.name,
                **self._getitem_frame_params,
            )

        # item must be Series type
        node = self._add_filter_operation(
            item=self, mask=item, node_output_type=NodeOutputType.FRAME
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=self.columns_info,
            node_name=node.name,
            **self._getitem_frame_params,
        )


class Frame(FrozenFrame):
    """
    Frame is a mutable version of the FrozenFrame class. It is used to represent a table in the query graph.
    This class supports column assignment to the table.
    """

    # class variables
    _series_class: ClassVar[Any] = Series

    @typechecked
    def __setitem__(
        self,
        key: Union[str, Tuple[FrozenSeries, str]],
        value: Union[int, float, str, bool, FrozenSeries],
    ) -> None:
        """
        Assign a scalar value or Series object of the same `dtype` to the `Frame` object

        Parameters
        ----------
        key: Union[str, Tuple[FrozenSeries, str]]
            column name to store the item. Alternatively, if a tuple is passed in, we will perform the masking
            operation.
        value: Union[int, float, str, bool, FrozenSeries]
            value to be assigned to the column

        Raises
        ------
        ValueError
            when the row indices between the Frame object & value object are not aligned
        """
        # This is required in order to allow us to support the
        # view[mask, column_name] = new_value
        # syntax
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError(f"{len(key)} elements found, when we only expect 2.")

            # subset the column to get the type first
            column_name: str = key[1]
            column = self[column_name]

            # check whether the mask has the expected types
            mask = key[0]
            if type(mask) not in {Series, FrozenSeries}:
                class_name = type(column).__name__
                raise ValueError(f"The mask provided should be a {class_name}.")

            assert isinstance(column, Series)
            column[mask] = value
            return

        if isinstance(value, FrozenSeries):
            if self.row_index_lineage != value.row_index_lineage:
                raise ValueError(f"Row indices between '{self}' and '{value}' are not aligned!")
            node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node, value.node],
            )
            column_info = ColumnInfo(name=key, dtype=value.dtype)
        else:
            node = self.graph.add_operation(
                node_type=NodeType.ASSIGN,
                node_params={"value": value, "name": key},
                node_output_type=NodeOutputType.FRAME,
                input_nodes=[self.node],
            )
            column_info = ColumnInfo(name=key, dtype=self.pytype_dbtype_map[type(value)])

        # update columns_info
        columns_info = [col for col in self.columns_info if col.name != key]
        columns_info.append(column_info)
        self.columns_info = columns_info

        # update node_name
        self.node_name = node.name

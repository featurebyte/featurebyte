"""
View class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Type, TypeVar, Union

from abc import ABC, abstractmethod

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    is_column_name_in_columns,
    join_column_lineage_map,
    join_tabular_data_ids,
    update_column_lineage_map_with_suffix,
)
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.mixin import SampleMixin
from featurebyte.core.series import Series
from featurebyte.core.util import append_to_lineage
from featurebyte.exception import NoJoinKeyFoundError, RepeatedColumnNamesError
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo
from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from featurebyte.api.groupby import GroupBy
else:
    GroupBy = TypeVar("GroupBy")

ViewT = TypeVar("ViewT", bound="View")


class ViewColumn(Series, SampleMixin):
    """
    ViewColumn class that is the base class of columns returned from any View (e.g. EventView)
    """

    _parent: Optional[View] = PrivateAttr(default=None)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> Optional[str]:
        if not self._parent:
            return None
        return self._parent.timestamp_column

    def binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method

        Parameters
        ----------
        other: Series
            Other Series object

        Returns
        -------
        dict[str, Any]
        """
        _ = other
        return {"tabular_data_ids": self.tabular_data_ids}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"tabular_data_ids": self.tabular_data_ids}


class GroupByMixin:
    """
    Mixin that provides groupby functionality to a View object
    """

    @typechecked
    def groupby(self, by_keys: Union[str, List[str]], category: Optional[str] = None) -> GroupBy:
        """
        Group View using a column or list of columns of the View object
        Refer to [GroupBy](/reference/featurebyte.api.groupby.GroupBy/)

        Parameters
        ----------
        by_keys: Union[str, List[str]]
            Define the key (entity) to for the `groupby` operation
        category : Optional[str]
            Optional category parameter to enable aggregation per category. It should be a column
            name in the View.

        Returns
        -------
        GroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.groupby import GroupBy

        return GroupBy(obj=self, keys=by_keys, category=category)  # type: ignore

    def validate_aggregation_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for groupby and aggregate
        functions

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated
        """


class View(ProtectedColumnsQueryObject, Frame, ABC):
    """
    View class that is the base class of any View (e.g. EventView)
    """

    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def __str__(self) -> str:
        return repr(self)

    @classmethod
    @typechecked
    def from_data(cls: Type[ViewT], data: DataApiObject, **kwargs: Any) -> ViewT:
        """
        Construct a View object

        Parameters
        ----------
        data: DataApiObject
            EventData object used to construct a View object
        kwargs: dict
            Additional parameters to be passed to the View constructor

        Returns
        -------
        ViewT
            constructed View object
        """
        return cls(
            feature_store=data.feature_store,
            tabular_source=data.tabular_source,
            columns_info=data.columns_info,
            node_name=data.node_name,
            column_lineage_map={col.name: (data.node.name,) for col in data.columns_info},
            row_index_lineage=tuple(data.row_index_lineage),
            tabular_data_ids=[data.id],
            **kwargs,
        )

    @property
    def entity_columns(self) -> list[str]:
        """
        List of entity columns

        Returns
        -------
        list[str]
        """
        return [col.name for col in self.columns_info if col.entity_id]

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["entity_columns"]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {"tabular_data_ids": self.tabular_data_ids}

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {"tabular_data_ids": self.tabular_data_ids}

    @typechecked
    def __getitem__(self, item: Union[str, List[str], Series]) -> Union[Series, Frame]:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.inherited_columns.union(item))
        output = super().__getitem__(item)
        return output

    @typechecked
    def __setitem__(self, key: str, value: Union[int, float, str, bool, Series]) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Column '{key}' cannot be modified!")
        super().__setitem__(key, value)

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ----------
        other_view: View
            the other view that we are joining with
        """

    @abstractmethod
    def get_join_column(self) -> str:
        """
        Returns the join column

        Returns
        -------
        str
            the column name for the join key
        """

    def get_join_parameters(self, calling_view: View) -> dict[str, Any]:
        """
        Returns additional query node parameters for join operation

        Note that self is the other view in the join, not the calling view.

        Parameters
        ----------
        calling_view: View
            Calling view of the join

        Returns
        -------
        """
        _ = calling_view
        return {}

    def _update_metadata(
        self,
        new_node_name: str,
        joined_columns_info: List[ColumnInfo],
        joined_column_lineage_map: Dict[str, Tuple[str, ...]],
        joined_tabular_data_ids: List[PydanticObjectId],
    ) -> None:
        """
        Updates the metadata for the new join

        Parameters
        ----------
        new_node_name: str
            new node name
        joined_columns_info: List[ColumnInfo]
            joined columns info
        joined_column_lineage_map: Dict[str, Tuple[str, ...]]
            joined column lineage map
        joined_tabular_data_ids: List[PydanticObjectId]
            joined tabular data IDs
        """
        # Construct new row_index_lineage
        joined_row_index_lineage = append_to_lineage(self.row_index_lineage, new_node_name)

        self.node_name = new_node_name
        self.columns_info = joined_columns_info
        self.column_lineage_map = joined_column_lineage_map
        self.row_index_lineage = joined_row_index_lineage
        self.__dict__.update(
            {
                "tabular_data_ids": joined_tabular_data_ids,
            }
        )

    def _get_key_if_entity(self, other_view: View) -> Optional[tuple[str, str]]:
        """
        Returns a key if there's a match based on entity.

        Parameters
        ----------
        other_view: View
            the other view we are joining on

        Returns
        -------
        (str, str)
            the left and right columns to join on
        """
        other_join_key = other_view.get_join_column()
        # If the other join key is not an entity, skip this search.
        entity_id = None
        for col in other_view.columns_info:
            if col.entity_id and col.name == other_join_key:
                entity_id = col.entity_id

        if entity_id is None:
            return None

        # Find if there's a match. Check to see if there's only exactly one match. If there are multiple, return empty
        # and log a debug message.
        num_of_matches = 0
        calling_col_name = ""
        for col in self.columns_info:
            if col.entity_id == entity_id:
                num_of_matches += 1
                calling_col_name = col.name

        if num_of_matches == 0:
            return "", ""
        if num_of_matches == 1:
            return calling_col_name, other_join_key
        logger.debug(
            f"{num_of_matches} matches found for entity id {entity_id}. "
            f"Unable to automatically return a join key."
        )
        return None

    def _get_join_keys(self, other_view: View, on_column: Optional[str] = None) -> tuple[str, str]:
        """
        Returns the join keys of the two tables.

        Parameters
        ----------
        other_view: View
            the other view we are joining with
        on_column: Optional[str]
            the optional column we want to join on

        Returns
        -------
        tuple[str, str]
            the columns from the left and right tables that we want to join on

        Raises
        ------
        NoJoinKeyFoundError
            raised when no suitable join key has been found
        """
        if on_column is not None:
            return on_column, other_view.get_join_column()

        # Check if the keys are entities
        response = self._get_key_if_entity(other_view)
        if response is not None:
            return response[0], response[1]

        # Check that the target join column is present in the calling list of columns.
        # If it is not present, the name of the column of the calling view should be specified.
        other_join_key = other_view.get_join_column()
        if is_column_name_in_columns(other_join_key, self.columns_info):
            return other_join_key, other_join_key

        raise NoJoinKeyFoundError

    def _validate_join(self, other_view: View, rsuffix: str = "") -> None:
        """
        Main validate call for the join. This checks that
        - If there are overlapping column names but rsuffix is empty, should there be an error?
        - Calls the other validate_join function which can be overriden for implementation specific validation

        Parameters
        ----------
        other_view: View
            the other view that we are joining with
        rsuffix: str
            a suffix to append on to the right columns

        Raises
        ------
        RepeatedColumnNamesError
            raised when there are overlapping columns, but no rsuffix has been provided
        """
        # Validate whether there are overlapping column names
        if rsuffix == "":
            current_column_names = {col.name for col in self.columns_info}
            for other_col in other_view.columns_info:
                if other_col.name in current_column_names:
                    raise RepeatedColumnNamesError

        # Perform other validation
        self.validate_join(other_view)

    @typechecked
    def join(
        self,
        other_view: View,
        on: Optional[str] = None,  # pylint: disable=invalid-name
        how: Literal["left", "inner"] = "left",
        rsuffix: str = "",
    ) -> None:
        """
        Joins the current view with another view. Note that this other view should only be a SlowlyChangingView,
        or a DimensionView.

        Parameters
        ----------
        other_view: View
            the other view that we want to join with. This should only be a SlowlyChangingView, or DimensionView.
        on: Optional[str]
            - ‘on’ argument is optional if:
            - the name of the key column in the calling view is the same name as the natural (primary) key in the
              other view.
            - the primary key of the Dimension View or the natural key of the SCD View is an entity that has been
              tagged in the 2 views.
        how: str
            Argument is optional. Describes how we want to join the two views together. The default value is ‘left’,
            which indicates a left join.
        rsuffix: str
            Argument is used if the two views have overlapping column names and disambiguates such column names after
            join. The default rsuffix is an empty string - ''.
        """
        self.validate_join(other_view)

        left_input_columns = self.columns
        left_output_columns = self.columns

        right_input_columns = other_view.columns
        right_output_columns = append_rsuffix_to_columns(other_view.columns, rsuffix)
        left_on, right_on = self._get_join_keys(other_view, on)

        node_params = {
            "left_on": left_on,
            "right_on": right_on,
            "left_input_columns": left_input_columns,
            "left_output_columns": left_output_columns,
            "right_input_columns": right_input_columns,
            "right_output_columns": right_output_columns,
            "join_type": how,
        }
        node_params.update(other_view.get_join_parameters(self))

        node = self.graph.add_operation(
            node_type=NodeType.JOIN,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.node, other_view.node],
        )

        # Construct new columns_info
        joined_columns_info = combine_column_info_of_views(
            self.columns_info, append_rsuffix_to_column_info(other_view.columns_info, rsuffix)
        )

        # Construct new column_lineage_map
        updated_column_lineage_map_with_suffix = update_column_lineage_map_with_suffix(
            other_view.column_lineage_map, rsuffix
        )
        columns = list(updated_column_lineage_map_with_suffix.keys())
        joined_column_lineage_map = join_column_lineage_map(
            self.column_lineage_map, updated_column_lineage_map_with_suffix, columns, node.name
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, other_view.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(
            node.name, joined_columns_info, joined_column_lineage_map, joined_tabular_data_ids
        )

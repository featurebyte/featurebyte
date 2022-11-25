"""
View class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple, Type, TypeVar, Union

from abc import ABC

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.join_utils import (
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    join_column_lineage_map,
    join_tabular_data_ids,
)
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.core.util import append_to_lineage
from featurebyte.exception import NoJoinKeyFoundError
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo
from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from featurebyte.api.groupby import GroupBy
else:
    GroupBy = TypeVar("GroupBy")

ViewT = TypeVar("ViewT", bound="View")


class ViewColumn(Series):
    """
    ViewColumn class that is the base class of columns returned from any View (e.g. EventView)
    """

    _parent: Optional[View] = PrivateAttr(default=None)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

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


class GroupByMixin:  # pylint: disable=too-few-public-methods
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

    def validate_join(self, other_view: View):
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ---------
        other_view: View
            the other view that we are joining with
        """

    def get_join_column(self) -> str:
        """
        Returns the join column

        Returns
        -------
        str
            the column name for the join key
        """
        return ""

    def update_metadata(
        self,
        new_node_name: str,
        joined_columns_info: List[ColumnInfo],
        joined_column_lineage_map: Dict[str, Tuple[str, ...]],
        joined_tabular_data_ids: Any,
    ):
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
        joined_tabular_data_ids: Any
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

    @staticmethod
    def check_key_is_entity_in_view(view: View, column_name: str) -> bool:
        """
        Checks to see if a column name is an entity.

        Parameters
        ----------
        view: View
            the view we want to check
        column_name: str
            the column name we want to check if it's an entity

        Returns
        -------
        bool
        """
        entity_columns = view.entity_columns
        return column_name in entity_columns

    def check_if_key_is_entity_in_both(self, other_view: View, column_name: str) -> bool:
        """
        Checks if a column name is an entity in both views.

        Parameters
        ----------
        other_view: View
            the other view we want to check for
        column_name: str
            the column name we want to check if it's an entity

        Returns
        -------
        bool
        """
        key_is_entity_of_current_view = View.check_key_is_entity_in_view(self, column_name)
        key_is_entity_of_other_view = View.check_key_is_entity_in_view(other_view, column_name)
        return key_is_entity_of_current_view and key_is_entity_of_other_view

    def get_join_keys(self, other_view: View, on_column: Optional[str]) -> tuple[str, str]:
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
            return on_column, on_column

        current_join_key = self.get_join_column()
        other_join_key = other_view.get_join_column()
        # Return the existing keys if they match
        if current_join_key == other_join_key:
            return current_join_key, other_join_key

        # Check if the keys are entities
        if self.check_key_is_entity_in_view(other_view, current_join_key):
            return current_join_key, current_join_key
        if self.check_if_key_is_entity_in_both(other_view, other_join_key):
            return other_join_key, other_join_key

        raise NoJoinKeyFoundError

    @typechecked
    def join(
        self,
        other_view: View,
        on: Optional[str] = None,  # pylint: disable=invalid-name
        how: Literal["left", "inner"] = "left",
        rsuffix: str = "",
    ) -> None:
        """
        Joins the current view with another view.

        If product_table is a Dimension or SCD View and production_category is a SCD View, an error message is
        raised with the following message "columns from a SCD View can’t be added to a Dimension or SCD View"

        Parameters
        ----------
        other_view: View
            the other view that we want to join with
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
            join. The default rsuffix is ''.

        Returns
        -------
        tuple[str, str]
        """
        self.validate_join(other_view)

        left_input_columns = self.columns
        left_output_columns = self.columns

        right_input_columns = other_view.columns
        right_output_columns = append_rsuffix_to_columns(other_view.columns, rsuffix)
        left_on, right_on = self.get_join_keys(other_view, on)

        node = self.graph.add_operation(
            node_type=NodeType.JOIN,
            node_params={
                "left_on": left_on,
                "right_on": right_on,
                "left_input_columns": left_input_columns,
                "left_output_columns": left_output_columns,
                "right_input_columns": right_input_columns,
                "right_output_columns": right_output_columns,
                "join_type": how,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[other_view.node, self.node],
        )

        # Construct new columns_info
        joined_columns_info = combine_column_info_of_views(
            self.columns_info, other_view.columns_info
        )

        # Construct new column_lineage_map
        columns = list(other_view.column_lineage_map.keys())
        joined_column_lineage_map = join_column_lineage_map(
            self.column_lineage_map, other_view.column_lineage_map, columns, node.name
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, other_view.tabular_data_ids
        )

        # Update metadata
        self.update_metadata(
            node.name, joined_columns_info, joined_column_lineage_map, joined_tabular_data_ids
        )

"""
View class
"""
from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from abc import ABC, abstractmethod

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    filter_join_key_from_column,
    filter_join_key_from_column_info,
    filter_join_key_from_column_lineage_map,
    is_column_name_in_columns,
    join_column_lineage_map,
    join_tabular_data_ids,
    update_column_lineage_map_with_suffix,
)
from featurebyte.common.model_util import parse_duration_string
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.mixin import SampleMixin
from featurebyte.core.series import Series
from featurebyte.core.util import append_to_lineage
from featurebyte.exception import NoJoinKeyFoundError, RepeatedColumnNamesError
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.node.generic import ProjectNode

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

    @typechecked
    def as_feature(self, feature_name: str, offset: Optional[str] = None) -> Feature:
        """
        Create a lookup feature directly using this column

        Parameters
        ----------
        feature_name: str
            Feature name
        offset: str
            When specified, retrieve feature value as of this offset prior to the point-in-time

        Returns
        -------
        Feature

        Raises
        ------
        ValueError
            If the column is a temporary column not associated with any View
        """
        view = self._parent
        if view is None:
            raise ValueError(
                "as_feature is only supported for named columns in the View object. Consider"
                " assigning the Feature to the View before calling as_feature()."
            )
        input_column_name = cast(ProjectNode.Parameters, self.node.parameters).columns[0]
        view = cast(View, view[[input_column_name]])
        feature = view.as_features([feature_name], offset=offset)[feature_name]
        return cast(Feature, feature)


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
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return {self.get_join_column()}.union(self._get_additional_inherited_columns())

    def _get_additional_inherited_columns(self) -> set[str]:
        """
        Additional columns set to be added to inherited_columns. To be overridden by subclasses of
        View when necessary.

        Returns
        -------
        set[str]
        """
        return set()

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

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:
        """
        Returns additional query node parameters for join operation

        Note that self is the other view in the join, not the calling view.

        Parameters
        ----------
        calling_view: View
            Calling view of the join

        Returns
        -------
        dict[str, Any]
        """
        _ = calling_view
        return {}

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        """
        Returns any additional query node parameters for as_feature operation (LookupNode)

        This is a no-op unless the lookup is time-aware (currently only available for
        SlowlyChangingView)

        Parameters
        ----------
        offset : str
            Optional offset parameter

        Returns
        -------
        dict[str, Any]
        """
        if offset is not None:
            logger.warning("offset parameter is provided but has no effect")
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
        Optional[tuple[str, str]]
            the left and right columns to join on, or None if there isn't exactly one match.
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
            return None
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
        ValueError
            raised if the `on_column` passed in is an empty string
        """
        if on_column is not None:
            if on_column == "":
                raise ValueError(
                    "The `on` column should not be empty. Please provide a value for this parameter."
                )
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

        raise NoJoinKeyFoundError(
            "Unable to automatically find a default join column key based on:\n"
            "- matching entities, or\n"
            f"- the join column '{other_join_key}' in the target view as it is not present in the"
            f" calling view\n"
            f"Please consider adding the `on` parameter in `join()` to explicitly specify a "
            f"column to join on."
        )

    def _validate_join(
        self,
        other_view: View,
        rsuffix: str = "",
        on: Optional[str] = None,  # pylint: disable=invalid-name
    ) -> None:
        """
        Main validate call for the join. This checks that
        - If there are overlapping column names but rsuffix is empty, throw an error
        - the join column provided via `on` is present in the columns of the calling view
        - Calls the other validate_join function which can be overriden for implementation specific validation

        Parameters
        ----------
        other_view: View
            the other view that we are joining with
        rsuffix: str
            a suffix to append on to the right columns
        on: Optional[str]
            the column to join on

        Raises
        ------
        RepeatedColumnNamesError
            raised when there are overlapping columns, but no rsuffix has been provided
        NoJoinKeyFoundError
            raised when the on column provided, is not present in the columns
        """
        # Validate whether there are overlapping column names
        if rsuffix == "":
            left_join_key, _ = self._get_join_keys(other_view, on)
            current_column_names = {col.name for col in self.columns_info}
            repeated_column_names = []
            for other_col in other_view.columns_info:
                # Raise an error if the name is repeated, but it is not a join key
                if other_col.name in current_column_names and other_col.name != left_join_key:
                    repeated_column_names.append(other_col.name)
            if len(repeated_column_names) > 0:
                raise RepeatedColumnNamesError(
                    f"Duplicate column names {repeated_column_names} found between the "
                    "calling view, and the target view.\nTo resolve this error, do consider "
                    "setting the rsuffix parameter in `join()` to disambiguate the "
                    "resulting columns in the joined view."
                )

        # Validate whether the join column provided is present in the columns
        if on is not None:
            current_column_names = {col.name for col in self.columns_info}
            if on not in current_column_names:
                raise NoJoinKeyFoundError(
                    f"The `on` column name provided '{on}' is not found in the calling view. "
                    f"Please pick a valid column name from {sorted(current_column_names)} to join on."
                )

        # Perform other validation
        self.validate_join(other_view)

    @typechecked
    def join(  # pylint: disable=too-many-locals
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
        self._validate_join(other_view, rsuffix, on=on)

        left_input_columns = self.columns
        left_output_columns = self.columns

        left_on, right_on = self._get_join_keys(other_view, on)
        filtered_other_columns = filter_join_key_from_column(other_view.columns, right_on)
        right_input_columns = filtered_other_columns
        right_output_columns = append_rsuffix_to_columns(filtered_other_columns, rsuffix)

        node_params = {
            "left_on": left_on,
            "right_on": right_on,
            "left_input_columns": left_input_columns,
            "left_output_columns": left_output_columns,
            "right_input_columns": right_input_columns,
            "right_output_columns": right_output_columns,
            "join_type": how,
        }
        node_params.update(
            other_view._get_join_parameters(self)  # pylint: disable=protected-access
        )

        node = self.graph.add_operation(
            node_type=NodeType.JOIN,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.node, other_view.node],
        )

        # Construct new columns_info
        filtered_column_infos = filter_join_key_from_column_info(other_view.columns_info, right_on)
        joined_columns_info = combine_column_info_of_views(
            self.columns_info, append_rsuffix_to_column_info(filtered_column_infos, rsuffix)
        )

        # Construct new column_lineage_map
        filtered_lineage_map = filter_join_key_from_column_lineage_map(
            other_view.column_lineage_map, right_on
        )
        updated_column_lineage_map_with_suffix = update_column_lineage_map_with_suffix(
            filtered_lineage_map, rsuffix
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

    @typechecked
    def as_features(self, feature_names: List[str], offset: Optional[str] = None) -> FeatureGroup:
        """
        Create lookup features directly from the columns in the View

        Parameters
        ----------
        feature_names: list[str]
            Feature names
        offset: str
            When specified, retrieve feature values as of this offset prior to the point-in-time

        Returns
        -------
        FeatureGroup

        Raises
        ------
        ValueError
            If the length of feature_names does not match with the number of columns in the View
        """
        # pylint: disable=too-many-locals
        special_columns = set(self.protected_columns)
        input_column_names = [
            column.name for column in self.columns_info if column.name not in special_columns
        ]
        if len(feature_names) != len(input_column_names):
            raise ValueError(
                f"Length of feature_names should be {len(input_column_names)}, got"
                f" {len(feature_names)}. Consider selecting columns before calling as_features."
            )

        # Validate offset is valid if provided
        if offset is not None:
            try:
                parse_duration_string(offset)
            except ValueError as exc:
                raise ValueError(
                    f"Failed to parse the offset parameter. Error: {str(exc)}"
                ) from exc

        # Get entity_column
        entity_column = self.get_join_column()

        # Get serving_name
        columns_info = self.columns_info
        column_entity_map = {col.name: col.entity_id for col in columns_info if col.entity_id}
        if entity_column not in column_entity_map:
            raise ValueError(f'Column "{entity_column}" is not an entity!')
        entity_id = column_entity_map[entity_column]
        entity = Entity.get_by_id(entity_id)
        serving_name = entity.serving_name

        # Input column names
        additional_params = self._get_as_feature_parameters(offset=offset)
        lookup_node_params = {
            "input_column_names": input_column_names,
            "feature_names": feature_names,
            "entity_column": entity_column,
            "serving_name": serving_name,
            "entity_id": entity_id,
            **additional_params,
        }
        lookup_node = self.graph.add_operation(
            node_type=NodeType.LOOKUP,
            node_params=lookup_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.node],
        )
        features = []
        for input_column_name, feature_name in zip(input_column_names, feature_names):
            feature_node = self.graph.add_operation(
                node_type=NodeType.PROJECT,
                node_params={"columns": [feature_name]},
                node_output_type=NodeOutputType.SERIES,
                input_nodes=[lookup_node],
            )
            var_type = self.column_var_type_map[input_column_name]
            feature = Feature(
                name=feature_name,
                feature_store=self.feature_store,
                tabular_source=self.tabular_source,
                node_name=feature_node.name,
                dtype=var_type,
                row_index_lineage=(lookup_node.name,),
                tabular_data_ids=self.tabular_data_ids,
                entity_ids=[entity_id],
            )
            features.append(feature)

        return FeatureGroup(features)

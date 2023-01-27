"""
View class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Literal, Optional, Type, TypeVar, Union, cast

from abc import ABC, abstractmethod

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.entity import Entity
from featurebyte.api.feature import Feature
from featurebyte.api.feature_list import FeatureGroup
from featurebyte.api.join_utils import (
    append_rsuffix_to_column_info,
    append_rsuffix_to_columns,
    combine_column_info_of_views,
    filter_columns,
    filter_columns_info,
    is_column_name_in_columns,
    join_tabular_data_ids,
)
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.model_util import validate_offset_string
from featurebyte.common.typing import Scalar, ScalarSequence
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.mixin import SampleMixin
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.exception import (
    ChangeViewNoJoinColumnError,
    NoJoinKeyFoundError,
    RepeatedColumnNamesError,
)
from featurebyte.logger import logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import InputNode, ProjectNode

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

    def binary_op_series_params(self, other: Scalar | Series | ScalarSequence) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method

        Parameters
        ----------
        other: Scalar | Series | ScalarSequence
            Other object

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
        feature = view.as_features(
            [input_column_name],
            [feature_name],
            offset=offset,
        )[feature_name]
        return cast(Feature, feature)


class GroupByMixin:
    """
    Mixin that provides groupby functionality to a View object
    """

    __fbautodoc__ = FBAutoDoc(section=["View"])

    @typechecked
    def groupby(self, by_keys: Union[str, List[str]], category: Optional[str] = None) -> GroupBy:
        """
        Group a view using one or more columns.

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

        See Also
        --------
        - [GroupBy](/reference/featurebyte.api.groupby.GroupBy/): GroupBy object
        - [GroupBy.aggregate](/reference/featurebyte.api.groupby.GroupBy.aggregate/):
        Create feature from grouped aggregates
        - [GroupBy.aggregate_over](/reference/featurebyte.api.groupby.GroupBy.aggregate_over/):
        Create features from grouped aggregates over different time windows

        Examples
        --------
        Create GroupBy object from an event view
        >>> import featurebyte as fb
        >>> transactions_view = fb.EventView.from_event_data(transactions_data)  # doctest: +SKIP
        >>> transactions_view.groupby("AccountID")  # doctest: +SKIP
        GroupBy(EventView(node.name=input_1), keys=['AccountID'])
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.groupby import GroupBy

        return GroupBy(obj=self, keys=by_keys, category=category)  # type: ignore

    def validate_aggregate_over_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for aggregate_over groupby's.

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated
        """

    def validate_simple_aggregate_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for simple aggregation functions.

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
        node_name = data.node_name
        assert isinstance(data.node, InputNode)
        graph_node = data.table_data.construct_cleaning_recipe_node(input_node=data.node)
        if graph_node:
            inserted_graph_node = GlobalQueryGraph().add_node(
                node=graph_node, input_nodes=[data.node]
            )
            node_name = inserted_graph_node.name

        return cls(
            feature_store=data.feature_store,
            tabular_source=data.tabular_source,
            columns_info=data.columns_info,
            node_name=node_name,
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
        additional_columns = self._get_additional_inherited_columns()
        try:
            return {self.get_join_column()}.union(self._get_additional_inherited_columns())
        except ChangeViewNoJoinColumnError:
            return additional_columns

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
        joined_tabular_data_ids: List[PydanticObjectId]
            joined tabular data IDs
        """
        self.node_name = new_node_name
        self.columns_info = joined_columns_info
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
        left_join_key, _ = self._get_join_keys(other_view, on)
        current_column_names = {col.name for col in self.columns_info}
        repeated_column_names = []
        for other_col in append_rsuffix_to_column_info(other_view.columns_info, rsuffix):
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

    def get_excluded_columns_as_other_view(self, join_key: str) -> list[str]:
        """
        Get the columns to be excluded from the view when it is used as other_view in a join. By
        default, join key is always excluded. Specific views can opt to exclude additional columns
        by overriding _get_additional_excluded_columns_as_other_view().

        Parameters
        ----------
        join_key: str
            Join key

        Returns
        -------
        list[str]
            List of column names to be excluded
        """
        excluded_columns = [join_key]
        excluded_columns.extend(self._get_additional_excluded_columns_as_other_view())
        return excluded_columns

    def _get_additional_excluded_columns_as_other_view(self) -> list[str]:
        return []

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
            Column name in the caller to join on the index in other_view. ‘on’ argument is optional if:
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
        other_view_excluded_columns = other_view.get_excluded_columns_as_other_view(right_on)
        filtered_other_columns = filter_columns(
            other_view.columns, exclude_columns=other_view_excluded_columns
        )
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
        filtered_column_infos = filter_columns_info(
            other_view.columns_info, other_view_excluded_columns
        )
        joined_columns_info = combine_column_info_of_views(
            self.columns_info, append_rsuffix_to_column_info(filtered_column_infos, rsuffix)
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, other_view.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(node.name, joined_columns_info, joined_tabular_data_ids)

    @staticmethod
    def _validate_offset(offset: Optional[str]) -> None:
        # Validate offset is valid if provided
        if offset is not None:
            validate_offset_string(offset)

    def _project_feature_from_node(
        self,
        node: Node,
        feature_name: str,
        feature_dtype: DBVarType,
        entity_ids: List[PydanticObjectId],
    ) -> Feature:
        """
        Create a Feature object from a node that produces features, such as groupby, lookup, etc.

        Parameters
        ----------
        node: Node
            Query graph node
        feature_name: str
            Feature name
        feature_dtype: DBVarType
            Variable type of the Feature
        entity_ids: List[PydanticObjectId]
            Entity ids associated with the Feature

        Returns
        -------
        Feature
        """
        feature_node = self.graph.add_operation(
            node_type=NodeType.PROJECT,
            node_params={"columns": [feature_name]},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node],
        )
        feature = Feature(
            name=feature_name,
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=feature_node.name,
            dtype=feature_dtype,
            tabular_data_ids=self.tabular_data_ids,
            entity_ids=entity_ids,
        )
        return feature

    def _validate_as_features_input_columns(
        self,
        column_names: list[str],
        feature_names: list[str],
    ) -> None:

        if len(column_names) == 0:
            raise ValueError("column_names is empty")

        for column in column_names:
            if column not in self.columns:
                raise ValueError(f"Column '{column}' not found")

        if len(set(feature_names)) != len(feature_names):
            raise ValueError("feature_names contains duplicated value(s)")

        if len(feature_names) != len(column_names):
            raise ValueError(
                f"Length of feature_names ({len(feature_names)}) should be the same as column_names"
                f" ({len(column_names)})"
            )

    def _get_input_node_for_lookup_node(self) -> Node:
        """
        Get the node before any projection(s) to be used as the input node for the lookup node in
        as_features(). The view before such projection(s) must also have those columns and can be
        used as the input instead. Removing redundant projections allows joins to be shared for
        lookup operations using the same source.

        Example:

        features_ab = view[columns].as_features(["a", "b"], ["FeatureA", "FeatureB"])
        features_c = view.as_features(["c"], ["FeatureC"])

        When features_ab and features_c are materialised in the same feature list, they can be
        retrieved using the same join query.

        Returns
        -------
        Node
        """
        # Find the first ancestor that is not a Project
        node_before_projection = self.node

        while node_before_projection.type == NodeType.PROJECT:
            input_node_names = self.graph.get_input_node_names(node_before_projection)
            assert len(input_node_names) == 1
            node_before_projection = self.graph.get_node_by_name(input_node_names[0])

        return node_before_projection

    @typechecked
    def as_features(
        self,
        column_names: List[str],
        feature_names: List[str],
        offset: Optional[str] = None,
    ) -> FeatureGroup:
        """
        Create lookup features directly from the columns in the View

        Parameters
        ----------
        column_names: List[str]
            Column names to be used to create the features
        feature_names: List[str]
            Feature names corresponding to column_names
        offset: Optional[str]
            When specified, retrieve feature values as of this offset prior to the point-in-time

        Raises
        ------
        ValueError
            When any of the specified parameters are invalid

        Returns
        -------
        FeatureGroup

        Examples
        --------
        >>> import featurebyte as fb
        >>> features = dimension_view.as_features(  # doctest: +SKIP
        ...    column_names=["column_a", "column_b"],
        ...    feature_names=["Feature A", "Feature B"],
        ... )
        >>> features.feature_names  # doctest: +SKIP
        ['Feature A', 'Feature B']
        """
        self._validate_as_features_input_columns(
            column_names=column_names,
            feature_names=feature_names,
        )

        self._validate_offset(offset)

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

        # Set up Lookup node
        additional_params = self._get_as_feature_parameters(offset=offset)
        lookup_node_params = {
            "input_column_names": column_names,
            "feature_names": feature_names,
            "entity_column": entity_column,
            "serving_name": serving_name,
            "entity_id": entity_id,
            **additional_params,
        }
        input_node = self._get_input_node_for_lookup_node()
        lookup_node = self.graph.add_operation(
            node_type=NodeType.LOOKUP,
            node_params=lookup_node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[input_node],
        )
        features = []
        for input_column_name, feature_name in zip(column_names, feature_names):
            feature = self._project_feature_from_node(
                node=lookup_node,
                feature_name=feature_name,
                feature_dtype=self.column_var_type_map[input_column_name],
                entity_ids=[entity_id],
            )
            features.append(feature)

        return FeatureGroup(features)

"""
EventView class
"""
from __future__ import annotations

from typing import Any, List, Optional, Union, cast

import copy

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.feature import Feature
from featurebyte.api.join_utils import join_tabular_data_ids
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.util import append_to_lineage
from featurebyte.enum import TableDataType
from featurebyte.exception import EventViewMatchingEntityColumnNotFound
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature_store import ColumnInfo
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import InputNode


class EventViewColumn(ViewColumn):
    """
    EventViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])

    @typechecked
    def lag(self, entity_columns: Union[str, List[str]], offset: int = 1) -> EventViewColumn:
        """
        Lag operation

        Parameters
        ----------
        entity_columns : str | list[str]
            Entity columns used when retrieving the lag value
        offset : int
            The number of rows backward from which to retrieve a value. Default is 1.

        Returns
        -------
        EventViewColumn

        Raises
        ------
        ValueError
            If a lag operation has already been applied to the column
        """
        if not isinstance(entity_columns, list):
            entity_columns = [entity_columns]
        if NodeType.LAG in self.node_types_lineage:
            raise ValueError("lag can only be applied once per column")
        assert self._parent is not None

        timestamp_column = self._parent.timestamp_column
        assert timestamp_column
        required_columns = entity_columns + [timestamp_column]
        input_nodes = [self.node]
        for col in required_columns:
            input_nodes.append(self._parent[col].node)

        node = self.graph.add_operation(
            node_type=NodeType.LAG,
            node_params={
                "entity_columns": entity_columns,
                "timestamp_column": self._parent.timestamp_column,
                "offset": offset,
            },
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=node.name,
            name=None,
            dtype=self.dtype,
            row_index_lineage=self.row_index_lineage,
            **self.unary_op_series_params(),
        )


class EventView(View, GroupByMixin):
    """
    EventView class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["View"], proxy_class="featurebyte.EventView")

    _series_class = EventViewColumn

    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_id_column: Optional[str] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event data

        Returns
        -------
        str
        """
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.EVENT_DATA
        )
        return input_node.parameters.timestamp  # type: ignore

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return {self.timestamp_column}

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + ["timestamp_column"]

    @classmethod
    @typechecked
    def from_event_data(cls, event_data: EventData) -> EventView:
        """
        Construct an EventView object

        Parameters
        ----------
        event_data: EventData
            EventData object used to construct EventView object

        Returns
        -------
        EventView
            constructed EventView object
        """
        return cls.from_data(
            event_data,
            default_feature_job_setting=event_data.default_feature_job_setting,
            event_id_column=event_data.event_id_column,
        )

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "default_feature_job_setting": self.default_feature_job_setting,
                "event_id_column": self.event_id_column,
            }
        )
        return params

    def get_join_column(self) -> str:
        # This is potentially none for backwards compatibility.
        # We can remove this once DEV-556 is done.
        assert self.event_id_column is not None
        return self.event_id_column

    def _validate_entity_col_override(self, entity_col: str) -> None:
        """
        Validates the entity_col override

        Parameters
        ----------
        entity_col: str
            entity column override to use

        Raises
        ------
        ValueError
            raised when the entity_col provided is an empty string, or if it's not a column on the event view
        """
        if entity_col == "":
            raise ValueError(
                "Entity column override provided is an empty string. Please provide a specific column."
            )

        # Check that the column is an entity column in the view.
        current_columns = {col.name for col in self.columns_info}
        if entity_col not in current_columns:
            raise ValueError(
                f"Entity column {entity_col} provided is not a column in the event view. Please pick one"
                f"of the following columns as an override: {sorted(current_columns)}"
            )

    @staticmethod
    def _is_time_based(feature: Feature) -> bool:
        """
        Checks to see if a feature is time based.

        Parameters
        ----------
        feature: Feature
            feature to check

        Returns
        -------
        bool
            returns True if the feature is time based, False if otherwise.
        """
        operation_structure = feature.extract_operation_structure()
        for aggregation in operation_structure.aggregations:
            if aggregation.window is not None:
                return True
        return False

    def _validate_feature_addition(
        self, feature: Feature, entity_col_override: Optional[str]
    ) -> None:
        """
        Validates feature addition
        - Checks that the feature is non-time based
        - Checks that entity is present in one of the columns

        Parameters
        ----------
        feature: Feature
            the feature we want to add on to the EventView
        entity_col_override: Optional[str]
            The entity column to use in the EventView. The type of this entity should match the entity of the feature.

        Raises
        ------
        ValueError
            raised when a time-based feature is passed in, or the entity_col_override validation fails
        """
        # Validate whether feature is time based
        if EventView._is_time_based(feature):
            raise ValueError("We currently only support the addition of non-time based features.")

        # Validate entity_col_override
        if entity_col_override is not None:
            self._validate_entity_col_override(entity_col_override)

    @staticmethod
    def _get_feature_entity_col(feature: Feature) -> str:
        """
        Get the entity column of the feature.

        Parameters
        ----------
        feature: Feature
            feature to get entity column for

        Returns
        -------
        str
            entity column name

        Raises
        ------
        ValueError
            raised when the feature is created from more than one entity
        """
        entity_columns = feature.entity_identifiers
        if len(entity_columns) != 1:
            raise ValueError(
                "The feature should only be based on one entity. We are currently unable to add features "
                "that are created from more, or less than one entity."
            )
        return entity_columns[0]

    @staticmethod
    def _get_feature_entity_id(feature: Feature) -> PydanticObjectId:
        """
        Get the entity ID of the feature.

        Parameters
        ----------
        feature: Feature
            feature to get entity column for

        Returns
        -------
        PydanticObjectId
            entity ID

        Raises
        ------
        ValueError
            raised when the feature is created from more than one entity
        """
        entity_ids = feature.entity_ids
        if len(entity_ids) != 1:
            raise ValueError(
                "The feature should only be based on one entity. We are currently unable to add features "
                "that are created from more than one entity."
            )
        return entity_ids[0]

    def _get_col_with_entity_id(self, entity_id: PydanticObjectId) -> Optional[str]:
        """
        Tries to find a single column with the matching entity ID.

        Parameters
        ----------
        entity_id: PydanticObjectId
            entity ID to search for

        Returns
        -------
        Optional[str]
            the column name with matching entity ID, None if no matches are found, or if there are multiple matches.
        """
        num_of_matches = 0
        column_name_to_use = None
        for col in self.columns_info:
            if col.entity_id == entity_id:
                num_of_matches += 1
                column_name_to_use = col.name
        # If we find multiple matches, return None.
        # This should throw an error down the line later if we don't find any matches. We don't raise an error
        # here in case we add other ways to find a column to join on, and as such defer to the caller to raise an
        # error as needed.
        if num_of_matches > 1:
            return None
        return column_name_to_use

    def _get_view_entity_column(self, feature: Feature, entity_column: Optional[str]) -> str:
        """
        Get the view entity column.

        Parameters
        ----------
        feature: Feature
            the feature we want to add to the EventView
        entity_column: Optional[str]
            The entity column to use in the EventView. The type of this entity should match the entity of the feature.

        Returns
        -------
        str
            the column name of the view to use

        Raises
        ------
        EventViewMatchingEntityColumnNotFound
            raised when we are unable to find a matching entity column automatically
        """
        # If we provide an entity column, use the entity column. We assume the column is not an empty string due to
        # validation done beforehand.
        if entity_column is not None:
            return entity_column

        # Try to match the entity column of the feature, with that of an entity in the view
        entity_feature_id = EventView._get_feature_entity_id(feature)
        feature_entity_col = self._get_col_with_entity_id(entity_feature_id)
        if feature_entity_col is not None:
            return feature_entity_col

        raise EventViewMatchingEntityColumnNotFound(
            "Unable to find a matching entity column. Please specify a column "
            f"from the EventView columns: {sorted(self.columns)}"
        )

    @typechecked
    def add_feature(
        self, new_column_name: str, feature: Feature, entity_column: Optional[str] = None
    ) -> None:
        """
        Features that are non-time based and are extracted from other data views can be added as a column to an event
        view if one of its columns has been tagged with the same entity as the entity of the features.

        Time-based features will be supported in the future once we have support for offline stores.

        Parameters
        ----------
        new_column_name: str
            the new column name to be added to the EventView
        feature: Feature
            the feature we want to add to the EventView
        entity_column: Optional[str]
            The entity column to use in the EventView. The type of this entity should match the entity of the feature.
        """
        # Validation
        self._validate_feature_addition(feature, entity_column)

        # Add join node
        view_entity_column = self._get_view_entity_column(feature, entity_column)
        node_params = {
            "view_entity_column": view_entity_column,
            "feature_entity_column": EventView._get_feature_entity_col(feature),
            "name": new_column_name,
        }
        node = self.graph.add_operation(
            node_type=NodeType.JOIN_FEATURE,
            node_params=node_params,
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[self.node, feature.node],
        )

        # Construct new columns_info
        updated_columns_info = copy.deepcopy(self.columns_info)
        updated_columns_info.append(
            ColumnInfo(
                name=new_column_name,
                dtype=feature.dtype,
                entity_id=feature.entity_identifiers[0],
            )
        )

        # Construct new column_lineage_map
        updated_column_lineage_map = copy.deepcopy(self.column_lineage_map)
        for col, lineage in updated_column_lineage_map.items():
            # TODO: should this be `feature.node.name` or `node.name` (i.e. the new node)?
            updated_column_lineage_map[col] = append_to_lineage(lineage, feature.node.name)

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, feature.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(
            node.name, updated_columns_info, updated_column_lineage_map, joined_tabular_data_ids
        )

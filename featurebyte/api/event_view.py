"""
EventView class
"""
from __future__ import annotations

from typing import Any, ClassVar, List, Literal, Optional, cast

import copy

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.api.feature import Feature
from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.view import GroupByMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.join_utils import join_tabular_data_ids
from featurebyte.enum import TableDataType, ViewMode
from featurebyte.exception import EventViewMatchingEntityColumnNotFound
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import EventTableData
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ColumnCleaningOperation, ViewMetadata


class EventViewColumn(LaggableViewColumn):
    """
    EventViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class EventView(View, GroupByMixin):
    """
    EventViews allow users to transform EventData to support the data preparation necessary before creating features.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["View"], proxy_class="featurebyte.EventView")

    # class variables
    _series_class = EventViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.EVENT_VIEW

    # pydantic instance variables
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
        return input_node.parameters.timestamp_column  # type: ignore

    def _get_additional_inherited_columns(self) -> set[str]:
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
    def from_event_data(
        cls,
        event_data: EventData,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> EventView:
        """
        Construct an EventView object

        Parameters
        ----------
        event_data: EventData
            EventData object used to construct EventView object
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data and the record creation date column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[featurebyte.query_graph.node.nested.ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        EventView
            constructed EventView object
        """
        cls._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the data node. The final graph looks like this:
        #    +-----------+     +----------------------------+
        #    | InputNode + --> | GraphNode(type:event_view) +
        #    +-----------+     +----------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and event_data.record_creation_date_column:
            drop_column_names.append(event_data.record_creation_date_column)

        data_node = event_data.frame.node
        assert isinstance(data_node, InputNode)
        event_table_data = cast(EventTableData, event_data.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        if view_mode == ViewMode.MANUAL:
            event_table_data = event_table_data.clone(
                column_cleaning_operations=column_cleaning_operations
            )
        else:
            column_cleaning_operations = [
                ColumnCleaningOperation(
                    column_name=col.name,
                    cleaning_operations=col.critical_data_info.cleaning_operations,
                )
                for col in event_table_data.columns_info
                if col.critical_data_info and col.critical_data_info.cleaning_operations
            ]

        view_graph_node, columns_info = event_table_data.construct_event_view_graph_node(
            event_data_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return EventView(
            feature_store=event_data.feature_store,
            tabular_source=event_data.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[event_data.id],
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

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
            }
        }

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

    def _validate_column_is_not_used(self, new_column_name: str) -> None:
        """
        Validate that the new column name passed in isn't an existing column name.

        Parameters
        ----------
        new_column_name: str
            the new column name that we want to use

        Raises
        ------
        ValueError
            raised when the new column name is already the name of an existing column
        """
        for column_name in self.columns:
            if column_name == new_column_name:
                raise ValueError(
                    "New column name provided is already a column in the existing view. Please pick a "
                    "different name."
                )

    def _validate_feature_addition(
        self, new_column_name: str, feature: Feature, entity_col_override: Optional[str]
    ) -> None:
        """
        Validates feature addition
        - Checks that the feature is non-time based
        - Checks that entity is present in one of the columns

        Parameters
        ----------
        new_column_name: str
            the new column name we want to use
        feature: Feature
            the feature we want to add on to the EventView
        entity_col_override: Optional[str]
            The entity column to use in the EventView. The type of this entity should match the entity of the feature.

        Raises
        ------
        ValueError
            raised when a time-based feature is passed in, or the entity_col_override validation fails
        """
        # Validate whether the new column name is used
        self._validate_column_is_not_used(new_column_name)

        # Validate whether feature is time based
        if feature.is_time_based:
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
        self._validate_feature_addition(new_column_name, feature, entity_column)

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
                entity_id=EventView._get_feature_entity_id(feature),
            )
        )

        # Construct new tabular_data_ids
        joined_tabular_data_ids = join_tabular_data_ids(
            self.tabular_data_ids, feature.tabular_data_ids
        )

        # Update metadata
        self._update_metadata(node.name, updated_columns_info, joined_tabular_data_ids)

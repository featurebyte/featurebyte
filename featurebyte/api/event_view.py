"""
EventView class
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

from bson import ObjectId
from pydantic import Field

from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.view import GroupByMixin, RawMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.exception import EventViewMatchingEntityColumnNotFound
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSettingUnion,
)
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.node.input import EventTableInputNodeParameters, InputNode
from featurebyte.typing import validate_type_is_feature

if TYPE_CHECKING:
    from featurebyte.api.feature import Feature


class EventViewColumn(LaggableViewColumn):
    """
    EventViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class EventView(View, GroupByMixin, RawMixin):
    """
    An EventView object is a modified version of the EventTable object that provides additional capabilities for
    transforming data. With an EventView, you can create and transform columns, extract lags and filter records
    prior to feature declaration.

    Event views are typically used to create Lookup features for the event entity, to create Aggregate Over a
    Window features for other entities or enrich the item data by joining to the related Item view.

    See Also
    --------
    - [event_table#get_view](/reference/featurebyte.api.event_table.EventTable.get_view/): get event view from an `EventTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.EventView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = EventViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.EVENT_VIEW

    # pydantic instance variables
    default_feature_job_setting: Optional[FeatureJobSettingUnion] = Field(
        frozen=True,
        description="Returns the default feature job setting for the view.\n\n"
        "The Default Feature Job Setting establishes the default setting used by "
        "features that aggregate data in the view, ensuring consistency of the "
        "Feature Job Setting across features created by different team members. "
        "While it's possible to override the setting during feature declaration, "
        "using the Default Feature Job Setting simplifies the process of setting "
        "up the Feature Job Setting for each feature.",
    )
    event_id_column: Optional[str] = Field(
        frozen=True,
        description="Returns the name of the column representing the event key of the Event view.",
    )
    event_timestamp_schema: Optional[TimestampSchema] = Field(frozen=True)

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event table

        Returns
        -------
        str
        """
        return self._get_event_table_node_parameters().timestamp_column  # type: ignore

    @property
    def timestamp_timezone_offset_column(self) -> Optional[str]:
        """
        Timestamp timezone offset column of the event table

        Returns
        -------
        Optional[str]
        """
        return self._get_event_table_node_parameters().event_timestamp_timezone_offset_column

    @property
    def timestamp_timezone_offset(self) -> Optional[str]:
        """
        Timestamp timezone of the event table

        Returns
        -------
        Optional[str]
        """
        return self._get_event_table_node_parameters().event_timestamp_timezone_offset

    def _get_event_table_node_parameters(self) -> EventTableInputNodeParameters:
        input_node = next(
            node
            for node in self.graph.iterate_nodes(target_node=self.node, node_type=NodeType.INPUT)
            if cast(InputNode, node).parameters.type == TableDataType.EVENT_TABLE
        )
        return cast(EventTableInputNodeParameters, input_node.parameters)

    def _get_additional_inherited_columns(self) -> set[str]:
        columns = {self.timestamp_column}
        if self.timestamp_timezone_offset_column is not None:
            columns.add(self.timestamp_timezone_offset_column)
        if self.event_timestamp_schema is not None and isinstance(
            self.event_timestamp_schema.timezone, TimeZoneColumn
        ):
            columns.add(self.event_timestamp_schema.timezone.column_name)
        return columns

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        out = super().protected_attributes + ["timestamp_column"]
        if self.timestamp_timezone_offset_column is not None:
            out.append("timestamp_timezone_offset_column")
        return out

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update({
            "default_feature_job_setting": self.default_feature_job_setting,
            "event_id_column": self.event_id_column,
            "event_timestamp_schema": self.event_timestamp_schema,
        })
        return params

    def get_join_column(self) -> str:
        # This is potentially none for backwards compatibility.
        # We can remove this once DEV-556 is done.
        join_column = self._get_join_column()
        assert join_column is not None, "Event ID column is not available."
        return join_column

    def _get_join_column(self) -> Optional[str]:
        return self.event_id_column

    def get_additional_lookup_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        _ = offset
        return {
            "event_parameters": {
                "event_timestamp_column": self.timestamp_column,
                "event_timestamp_metadata": self.operation_structure.get_dtype_metadata(
                    column_name=self.timestamp_column
                ),
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
        - Checks that the feature does not use request columns
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

        # Validate whether request column is used in feature definition
        if feature.used_request_column:
            raise ValueError(
                "We currently only support the addition of features that do not use request columns."
            )

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
        entity_columns = feature.graph.get_entity_columns(node_name=feature.node_name)
        if len(entity_columns) != 1:
            raise ValueError(
                "The feature should only be based on one entity. We are currently unable to add features "
                "that are created from more, or less than one entity."
            )
        return entity_columns[0]

    @staticmethod
    def _get_feature_entity_id(feature: Feature) -> ObjectId:
        """
        Get the entity ID of the feature.

        Parameters
        ----------
        feature: Feature
            feature to get entity column for

        Returns
        -------
        ObjectId
            entity ID

        Raises
        ------
        ValueError
            raised when the feature is created from more than one entity
        """
        entity_ids = feature.graph.get_entity_ids(node_name=feature.node_name)
        if len(entity_ids) != 1:
            raise ValueError(
                "The feature should only be based on one entity. We are currently unable to add features "
                "that are created from more than one entity."
            )
        return entity_ids[0]

    def _get_col_with_entity_id(self, entity_id: ObjectId) -> Optional[str]:
        """
        Tries to find a single column with the matching entity ID.

        Parameters
        ----------
        entity_id: ObjectId
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

    def add_feature(
        self, new_column_name: str, feature: Feature, entity_column: Optional[str] = None
    ) -> EventView:
        """
        Adds a simple aggregate feature obtained from an Item View to the corresponding Event View. Once the feature
        is integrated in this manner, it can be aggregated as any other column over a time frame to create Aggregate
        Over a Window features.

        For example, one can calculate a customer's average order size over the last three weeks by using the order
        size feature extracted from the Order Items view and aggregating it over that time frame in the related
        Order view.

        Parameters
        ----------
        new_column_name: str
            The new column name to be added to the EventView.
        feature: Feature
            The feature we want to add to the EventView.
        entity_column: Optional[str]
            The column representing the primary entity of the added feature in the EventView.

        Returns
        -------
        EventView
            The EventView with the new feature added.

        Examples
        --------
        Add feature to an EventView.

        >>> items_view = catalog.get_view("INVOICEITEMS")
        >>> # Group items by the column GroceryInvoiceGuid that references the customer entity
        >>> items_by_invoice = items_view.groupby("GroceryInvoiceGuid")  # doctest: +SKIP
        >>> # Get the number of items in each invoice
        >>> invoice_item_count = items_by_invoice.aggregate(  # doctest: +SKIP
        ...     None,
        ...     method=fb.AggFunc.COUNT,
        ...     feature_name="InvoiceItemCount",
        ... )
        >>> event_view = catalog.get_view("GROCERYINVOICE")  # doctest: +SKIP
        >>> event_view = event_view.add_feature(
        ...     "InvoiceItemCount", invoice_item_count
        ... )  # doctest: +SKIP
        """
        validate_type_is_feature(feature, "feature")

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
            )
        )

        # create a new view and return it
        return self._create_joined_view(
            new_node_name=node.name, joined_columns_info=updated_columns_info
        )

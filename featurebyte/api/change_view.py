"""
ChangeView class
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Tuple, cast

from dataclasses import dataclass
from datetime import datetime

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.scd_data import SlowlyChangingData
from featurebyte.api.view import GroupByMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import ChangeViewNoJoinColumnError
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.input import InputNode


class ChangeViewColumn(LaggableViewColumn):
    """
    ChangeViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


@dataclass
class ChangeViewColumnNames:
    """
    Representation of column names to use in the change view
    """

    previous_tracked_column_name: str
    new_tracked_column_name: str
    previous_valid_from_column_name: str
    new_valid_from_column_name: str


class ChangeView(View, GroupByMixin):
    """
    ChangeView is used to capture changes in slowly changing data in an easy manner. This is useful as changes in
    SCD data may constitute powerful features such as:\n
    - how many times has a customer moved in the past 6 months?\n
    - if they moved the past 6 months, where did they use to live?\n
    - did they get divorced recently?\n
    - did they have any new kids in the family?\n
    - do they have a new job?

    To support such important features, users can create a Change View from SCD Data.

    This new view tracks all changes for a given column. The resulting view has 5 columns:\n
    - past_valid_from_timestamp\n
    - new_valid_from_timestamp (which is the event timestamp of the event view and equal to the effective, or start,
      timestamp of the SCD)\n
    - the natural key of the SCD View\n
    - past_NAME_OF_COLUMN: value of the column before the change\n
    - new_NAME_OF_COLUMN: value of the column after the change

    To create this Change View, Users need to provide:\n
    - the name of the SCD data\n
    - the name of the SCD column for which they want to track changes

    Optionally, the default Feature Job Setting for the View. If non is provided, we will default to once a day, at the
    time of the creation of the view.

    Optionally, users can also provide a prefix parameter. This will allow users to specify a custom prefix for the new
    column values if they prefer.

    Features can be created the same way as features from an Event View.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ChangeView",
    )

    # class variables
    _series_class = ChangeViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.CHANGE_VIEW

    # pydantic instance variables
    default_feature_job_setting: FeatureJobSetting
    effective_timestamp_column: str = Field(allow_mutation=False)
    natural_key_column: str = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the change view

        Returns
        -------
        str
        """
        return self.effective_timestamp_column

    def _get_additional_inherited_columns(self) -> set[str]:
        return {self.timestamp_column}

    @staticmethod
    def _validate_prefixes(prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None) -> None:
        """
        Validate prefixes

        Parameters
        ----------
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Raises
        ------
        ValueError
            raised when any of the validation checks fails
        """
        if prefixes is None:
            return

        before, after = prefixes
        if before is None and after is None:
            raise ValueError(
                "Prefixes provided are both None. Please indicate at least one prefix to update."
            )
        if before == "" or after == "":
            raise ValueError(
                "Please provide a non-empty string as a prefix value. If you want to use the default "
                "value, please provide a `None` type instead."
            )
        if before == after:
            raise ValueError(
                f"Prefixes provided need to be different values. Current prefix value: {before}"
            )

    @staticmethod
    def _validate_inputs(
        scd_data: SlowlyChangingData,
        track_changes_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> None:
        """
        Validate the inputs.

        Checks
        - column is not empty
        - column is a column in the scd data table

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create view from
        track_changes_column: str
            column to track changes for
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Raises
        ------
        ValueError
            raised when any of the validation checks fails
        """
        if track_changes_column == "":
            raise ValueError("Empty column provided. Please provide a valid column.")
        if track_changes_column not in scd_data.columns:
            raise ValueError(
                "Column provided is not a column in the SlowlyChangingData provided. Please pick a column "
                f"from: {sorted(scd_data.columns)}."
            )

        # Validate prefixes
        ChangeView._validate_prefixes(prefixes)

    @staticmethod
    def _get_new_column_names(
        tracked_column: str,
        timestamp_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]],
    ) -> ChangeViewColumnNames:
        """
        Helper method to return the tracked column names.

        Parameters
        ----------
        tracked_column: str
            column we want to track
        timestamp_column: str
            column denoting the timestamp
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Returns
        -------
        ChangeViewColumnNames
            column names to use in the change view
        """
        old_prefix = "past_"
        new_prefix = "new_"
        if prefixes is not None:
            if prefixes[0] is not None:
                old_prefix = prefixes[0]
            if prefixes[1] is not None:
                new_prefix = prefixes[1]

        past_col_name = f"{old_prefix}{tracked_column}"
        new_col_name = f"{new_prefix}{tracked_column}"
        past_timestamp_col_name = f"{old_prefix}{timestamp_column}"
        new_timestamp_col_name = f"{new_prefix}{timestamp_column}"
        return ChangeViewColumnNames(
            previous_tracked_column_name=past_col_name,
            new_tracked_column_name=new_col_name,
            previous_valid_from_column_name=past_timestamp_col_name,
            new_valid_from_column_name=new_timestamp_col_name,
        )

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        params = super()._getitem_frame_params
        params.update(
            {
                "default_feature_job_setting": self.default_feature_job_setting,
                "natural_key_column": self.natural_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
            }
        )
        return params

    @classmethod
    @typechecked
    def from_slowly_changing_data(
        cls,
        scd_data: SlowlyChangingData,
        track_changes_column: str,
        default_feature_job_setting: Optional[FeatureJobSetting] = None,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> ChangeView:
        """
        Create a change view from SCD data.

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create view from
        track_changes_column: str
            column to track changes for
        default_feature_job_setting: Optional[FeatureJobSetting]
            default feature job setting to set
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Returns
        -------
        ChangeView
        """
        # Validate input
        ChangeView._validate_inputs(scd_data, track_changes_column, prefixes)

        # construct change view graph node from the scd data, the final graph looks like:
        #       +---------------------+    +-----------------------------+
        #       | InputNode(type:scd) | -->| GraphNode(type:change_view) |
        #       +---------------------+    +-----------------------------+
        feature_job_setting = ChangeView.get_default_feature_job_setting(
            default_feature_job_setting
        )
        col_names = ChangeView._get_new_column_names(
            track_changes_column, scd_data.effective_timestamp_column, prefixes
        )
        drop_columns_names = []
        if (
            scd_data.record_creation_date_column
            and scd_data.record_creation_date_column != track_changes_column
        ):
            drop_columns_names.append(scd_data.record_creation_date_column)

        data_node = scd_data.frame.node
        assert isinstance(data_node, InputNode)
        scd_table_data = cast(SCDTableData, scd_data.table_data)
        view_graph_node, columns_info = scd_table_data.construct_change_view_graph_node(
            scd_data_node=data_node,
            track_changes_column=track_changes_column,
            prefixes=prefixes,
            drop_column_names=drop_columns_names,
            metadata={
                "track_changes_column": track_changes_column,
                "default_feature_job_setting": default_feature_job_setting,
                "prefixes": prefixes,
            },
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return ChangeView(
            feature_store=scd_data.feature_store,
            tabular_source=scd_data.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[scd_data.id],
            natural_key_column=scd_data.natural_key_column,
            effective_timestamp_column=col_names.new_valid_from_column_name,
            default_feature_job_setting=feature_job_setting,
        )

    @typechecked
    def update_default_feature_job_setting(self, feature_job_setting: FeatureJobSetting) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            Feature job setting
        """
        self.default_feature_job_setting = feature_job_setting

    @staticmethod
    def get_default_feature_job_setting(
        feature_job_setting: Optional[FeatureJobSetting] = None,
    ) -> FeatureJobSetting:
        """
        Get default feature job setting. If none is provided, we'll set the default job setting to be once a day,
        at the time of the view creation.

        Parameters
        ----------
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting object (auto-detected if not provided)

        Returns
        -------
        FeatureJobSetting
            the feature job setting we want to use
        """
        if feature_job_setting is not None:
            return feature_job_setting
        # default job setting of once a day, at the time the view is created
        now = datetime.now()
        hour, minute = now.hour, now.minute
        return FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency=f"{hour}h{minute}m",
            frequency="24h",
        )

    def get_join_column(self) -> str:
        raise ChangeViewNoJoinColumnError

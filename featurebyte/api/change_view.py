"""
ChangeView class
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional, Tuple

from datetime import datetime

from pydantic import Field
from typeguard import typechecked

from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.scd_table import SCDTable
from featurebyte.api.view import GroupByMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import ChangeViewNoJoinColumnError
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting


class ChangeViewColumn(LaggableViewColumn):
    """
    ChangeViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ChangeView(View, GroupByMixin):
    """
    ChangeView is used to capture changes in SCDTable in an easy manner. This is useful as changes in
    SCDTable may constitute powerful features such as:\n
    - how many times has a customer moved in the past 6 months?\n
    - if they moved the past 6 months, where did they use to live?\n
    - did they get divorced recently?\n
    - did they have any new kids in the family?\n
    - do they have a new job?

    To support such important features, users can create a Change View from SCDTable.

    This new view tracks all changes for a given column. The resulting view has 5 columns:\n
    - past_valid_from_timestamp\n
    - new_valid_from_timestamp (which is the event timestamp of the event view and equal to the effective, or start,
      timestamp of the SCD)\n
    - the natural key of the SCD View\n
    - past_NAME_OF_COLUMN: value of the column before the change\n
    - new_NAME_OF_COLUMN: value of the column after the change

    To create this Change View, Users need to provide:\n
    - the name of the SCD table\n
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
    def validate_inputs(
        scd_data: SCDTable,
        track_changes_column: str,
        prefixes: Optional[Tuple[Optional[str], Optional[str]]] = None,
    ) -> None:
        """
        Validate the inputs.

        Checks
        - column is not empty
        - column is a column in the scd table

        Parameters
        ----------
        scd_data: SCDTable
            table to create view from
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
                "Column provided is not a column in the SCDTable provided. Please pick a column "
                f"from: {sorted(scd_data.columns)}."
            )

        # Validate prefixes
        ChangeView._validate_prefixes(prefixes)

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

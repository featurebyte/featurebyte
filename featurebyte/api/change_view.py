"""
ChangeView class
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

from pydantic import Field
from typeguard import typechecked

from featurebyte import FeatureJobSetting, SlowlyChangingData
from featurebyte.api.lag import LaggableViewColumn
from featurebyte.api.view import GroupByMixin, View
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import ChangeViewNoJoinColumnError


class ChangeViewColumn(LaggableViewColumn):
    """
    ChangeViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ChangeView(View, GroupByMixin):
    """
    ChangeView class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ChangeView",
    )

    _series_class = ChangeViewColumn

    default_feature_job_setting: FeatureJobSetting = Field(allow_mutation=False)
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
    def _validate_inputs(scd_data: SlowlyChangingData, track_changes_column: str) -> None:
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

    @staticmethod
    def _get_new_column_names(tracked_column: str) -> Tuple[str, str]:
        """
        Helper method to return the tracked column names.

        Parameters
        ----------
        tracked_column: str
            column we want to track

        Returns
        -------
        Tuple[str, str]
            old, and new column names
        """
        past_col_name = f"past_{tracked_column}"
        new_col_name = f"new_{tracked_column}"
        return past_col_name, new_col_name

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
    def from_scd_data(
        cls,
        scd_data: SlowlyChangingData,
        track_changes_column: str,
        default_feature_job_setting: Optional[FeatureJobSetting] = None,
    ) -> "ChangeView":
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

        Returns
        -------
        "ChangeView"
        """
        # Validate input
        ChangeView._validate_inputs(scd_data, track_changes_column)

        # Build view
        feature_job_setting = ChangeView.get_default_feature_job_setting(
            default_feature_job_setting
        )
        change_view = cls.from_data(
            scd_data,
            natural_key_column=scd_data.natural_key_column,
            effective_timestamp_column=scd_data.effective_timestamp_column,
            default_feature_job_setting=feature_job_setting,
        )
        past_col_name, new_col_name = ChangeView._get_new_column_names(track_changes_column)
        change_view[new_col_name] = change_view[track_changes_column]  # type: ignore
        change_view[past_col_name] = change_view[new_col_name].lag(change_view.natural_key_column)  # type: ignore

        # select the 4 cols we want to present
        change_view = change_view[
            [
                change_view.natural_key_column,
                change_view.effective_timestamp_column,
                new_col_name,
                past_col_name,
            ]
        ]  # type: ignore
        return change_view

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
        # default job setting of once a day
        return FeatureJobSetting(
            blind_spot="0",
            time_modulo_frequency="0",
            frequency="24h",
        )

    def get_join_column(self) -> str:
        raise ChangeViewNoJoinColumnError

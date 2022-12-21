"""
ChangeView class
"""
from __future__ import annotations

from typing import Any, Optional, Tuple

from datetime import datetime

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
        tracked_column: str, prefixes: Optional[Tuple[Optional[str], Optional[str]]]
    ) -> Tuple[str, str]:
        """
        Helper method to return the tracked column names.

        Parameters
        ----------
        tracked_column: str
            column we want to track
        prefixes: Optional[Tuple[Optional[str], Optional[str]]]
            Optional prefixes where each element indicates the prefix to add to the new column names for the name of
            the column that we want to track. The first prefix will be used for the old, and the second for the new.
            Pass a value of None instead of a string to indicate that the column name will be prefixed with the default
            values of "past_", and "new_". At least one of the values must not be None. If two values are provided,
            they must be different.

        Returns
        -------
        Tuple[str, str]
            old, and new column names
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
        "ChangeView"
        """
        # Validate input
        ChangeView._validate_inputs(scd_data, track_changes_column, prefixes)

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
        past_col_name, new_col_name = ChangeView._get_new_column_names(
            track_changes_column, prefixes
        )
        # We type:ignore these assignments as the right side variable has wrong type hints. We're looking to fix
        # this in DEV-918.
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

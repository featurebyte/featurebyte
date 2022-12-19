"""
ChangeView class
"""
from typing import Dict, List, Optional, Tuple

from pydantic import Field
from typeguard import typechecked

from featurebyte import FeatureJobSetting, SlowlyChangingData
from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.query_graph.model.column_info import ColumnInfo


class ChangeViewColumn(ViewColumn):
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

    @staticmethod
    def _validate_inputs(scd_data: SlowlyChangingData, column_to_track_changes: str) -> None:
        """
        Validate the inputs.

        Checks
        - column is not empty
        - column is a column in the scd data table

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create view from
        column_to_track_changes: str
            column to track changes for

        Raises
        ------
        ValueError
            raised when any of the validation checks fails
        """
        if column_to_track_changes == "":
            raise ValueError("Empty column provided. Please provide a valid column.")
        if column_to_track_changes not in scd_data.columns:
            raise ValueError(
                "Column provided is not a column in the SlowlyChangingData provided. Please pick a column "
                f"from: {sorted(scd_data.columns)}."
            )

    @staticmethod
    def _get_col_name_to_col_info(scd_data: SlowlyChangingData) -> Dict[str, ColumnInfo]:
        """
        Helper method to get a dict mapping column name to column info

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create columns for the ChangeView for

        Returns
        -------
        Dict[str, ColumnInfo]
            map from column name to the ColumnInfo
        """
        output = {}
        for col_info in scd_data.columns_info:
            output[col_info.name] = col_info
        return output

    @staticmethod
    def _copy_existing_columns_info_from_scd(scd_data: SlowlyChangingData) -> List[ColumnInfo]:
        """
        Helper method to take existing column info from the slowly changing data.

        Specifically, we take the following 2 columns from the SCD:
        - change_timestamp (which is the event timestamp of the event view and equal to the effective (or start)
          timestamp of the SCD)
        - the natural key of the SCD View

        The other columns will be added on separately.
        - past_NAME_OF_COLUMN: value of the column before the change
        - new_NAME_OF_COLUMN: value of the column after the change

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create columns for the ChangeView for

        Returns
        -------
        List[ColumnInfo]
            columns for the change view
        """
        col_name_to_col_info = ChangeView._get_col_name_to_col_info(scd_data)
        effective_timestamp_col_info = col_name_to_col_info[scd_data.effective_timestamp_column]
        natural_key_col_info = col_name_to_col_info[scd_data.natural_key_column]
        return [effective_timestamp_col_info, natural_key_col_info]

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

    @staticmethod
    @typechecked
    def from_scd_data(
        scd_data: SlowlyChangingData,
        column_to_track_changes: str,
        default_feature_job_setting: Optional[FeatureJobSetting] = None,
    ) -> "ChangeView":
        """
        Create a change view from SCD data.

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create view from
        column_to_track_changes: str
            column to track changes for
        default_feature_job_setting: Optional[FeatureJobSetting]
            default feature job setting to set

        Returns
        -------
        "ChangeView"
        """
        # Validate input
        ChangeView._validate_inputs(scd_data, column_to_track_changes)

        # Build view
        col_info = ChangeView._copy_existing_columns_info_from_scd(scd_data)
        feature_job_setting = ChangeView.get_default_feature_job_setting(
            default_feature_job_setting
        )
        change_view = ChangeView(
            feature_store=scd_data.feature_store,
            tabular_source=scd_data.tabular_source,
            columns_info=col_info,
            node_name=scd_data.node_name,
            column_lineage_map={col.name: (scd_data.node.name,) for col in col_info},
            row_index_lineage=tuple(scd_data.row_index_lineage),
            tabular_data_ids=[scd_data.id],
            # other params
            natural_key_column=scd_data.natural_key_column,
            effective_timestamp_column=scd_data.effective_timestamp_column,
            default_feature_job_setting=feature_job_setting,
        )
        past_col_name, new_col_name = ChangeView._get_new_column_names(column_to_track_changes)
        change_view[new_col_name] = scd_data.column_to_track_changes
        change_view[past_col_name] = change_view[new_col_name].lag(scd_data.natural_key_column)

        # Update the feature job setting
        change_view.update_default_feature_job_setting(default_feature_job_setting)
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
        # TODO:
        return self.natural_key_column

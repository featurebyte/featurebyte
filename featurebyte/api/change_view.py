"""
ChangeView class
"""
from typing import Dict, List, Optional

from pydantic import Field
from typeguard import typechecked

from featurebyte import FeatureJobSetting, SlowlyChangingData
from featurebyte.api.view import View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.query_graph.model.column_info import ColumnInfo


class ChangeViewColumn(ViewColumn):
    """
    ChangeViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class ChangeView(View):
    """
    ChangeView class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.ChangeView",
    )

    _series_class = ChangeViewColumn

    natural_key_column: str = Field(allow_mutation=False)

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
    def _create_column_info_from_scd(
        scd_data: SlowlyChangingData, column_to_track_changes: str
    ) -> List[ColumnInfo]:
        """
        Helper method to create column info from the slowly changing data.

        Specifically, the resulting ChangeView only has 4 columns:
        - change_timestamp (which is the event timestamp of the event view and equal to the effective (or start)
          timestamp of the SCD)
        - the natural key of the SCD View
        - past_NAME_OF_COLUMN: value of the column before the change
        - new_NAME_OF_COLUMN: value of the column after the change

        Parameters
        ----------
        scd_data: SlowlyChangingData
            data to create columns for the ChangeView for
        column_to_track_changes: str
            column to track changes for

        Returns
        -------
        List[ColumnInfo]
            columns for the change view
        """
        col_name_to_col_info = ChangeView._get_col_name_to_col_info(scd_data)
        effective_timestamp_col_info = col_name_to_col_info[scd_data.effective_timestamp_column]
        natural_key_col_info = col_name_to_col_info[scd_data.natural_key_column]
        tracked_column_col_info = col_name_to_col_info[column_to_track_changes]
        past_col_info = ColumnInfo(
            name=f"past_{tracked_column_col_info.name}",
            dtype=tracked_column_col_info.dtype,
            entity_id=tracked_column_col_info.entity_id,
            semantic_id=tracked_column_col_info.semantic_id,
        )
        new_col_info = ColumnInfo(
            name=f"new_{tracked_column_col_info.name}",
            dtype=tracked_column_col_info.dtype,
            entity_id=tracked_column_col_info.entity_id,
            semantic_id=tracked_column_col_info.semantic_id,
        )
        return [effective_timestamp_col_info, natural_key_col_info, past_col_info, new_col_info]

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
        # TODO: do stuff with default_feature_job_setting
        _ = default_feature_job_setting

        # Validate input
        ChangeView._validate_inputs(scd_data, column_to_track_changes)

        # Build view
        col_info = ChangeView._create_column_info_from_scd(scd_data, column_to_track_changes)
        return ChangeView(
            feature_store=scd_data.feature_store,
            tabular_source=scd_data.tabular_source,
            columns_info=col_info,
            node_name=scd_data.node_name,
            column_lineage_map={col.name: (scd_data.node.name,) for col in col_info},
            row_index_lineage=tuple(scd_data.row_index_lineage),
            tabular_data_ids=[scd_data.id],
            # other params
            natural_key_column=scd_data.natural_key_column,
        )

    def update_default_feature_job_setting(
        self, feature_job_setting: Optional[FeatureJobSetting] = None
    ) -> None:
        """
        Update default feature job setting

        Parameters
        ----------
        feature_job_setting: Optional[FeatureJobSetting]
            Feature job setting object (auto-detected if not provided)
        """
        # TODO: figure this out
        # if feature_job_setting is None:
        #     job_setting_analysis = self.post_async_task(
        #         route="/feature_job_setting_analysis", payload={"event_data_id": str(self.id)}
        #     )
        #     recommended_setting = job_setting_analysis["analysis_result"][
        #         "recommended_feature_job_setting"
        #     ]
        #     feature_job_setting = FeatureJobSetting(
        #         blind_spot=f'{recommended_setting["blind_spot"]}s',
        #         time_modulo_frequency=f'{recommended_setting["job_time_modulo_frequency"]}s',
        #         frequency=f'{recommended_setting["frequency"]}s',
        #     )
        # TODO: figure this out
        # self.update(
        #     update_payload={"default_feature_job_setting": feature_job_setting.dict()},
        #     allow_update_local=True,
        # )

    def get_join_column(self) -> str:
        return self.natural_key_column

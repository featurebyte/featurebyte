"""
FeatureJobSettingAnalysis class
"""
from __future__ import annotations

from typing import Optional, Union

from io import BytesIO
from pathlib import Path

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.common.env_util import display_html_in_notebook
from featurebyte.config import Configurations
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisRecord,
)


class FeatureJobSettingAnalysis(FeatureJobSettingAnalysisModel, ApiObject):
    """
    FeatureJobSettingAnalysis class
    """

    # class variables
    _route = "/feature_job_setting_analysis"
    _list_schema = FeatureJobSettingAnalysisRecord
    _get_schema = FeatureJobSettingAnalysisModel
    _list_fields = [
        "id",
        "created_at",
        "event_data",
        "analysis_start",
        "analysis_date",
        "frequency",
        "job_time_modulo_frequency",
        "blind_spot",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("event_data_id", TableApiObject, "event_data"),
    ]

    @classmethod
    @typechecked
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        records = super()._post_process_list(item_list)
        # format results into dataframe
        analysis_options = pd.json_normalize(records.analysis_options)
        recommendation = pd.json_normalize(records.recommended_feature_job_setting)

        return pd.concat(
            [
                records[["id", "created_at", "event_data"]],
                analysis_options,
                recommendation,
            ],
            axis=1,
        )

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        event_data_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        event_data_id: Optional[ObjectId]
            Event data id used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        params = {}
        if event_data_id:
            params = {"event_data_id": str(event_data_id)}
        return cls._list(include_id=include_id, params=params)

    @typechecked
    def display_report(self) -> None:
        """
        Display analysis report
        """
        display_html_in_notebook(self.analysis_report)

    @typechecked
    def download_report(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downlaod analysis report

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded report

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path
        """
        client = Configurations().get_client()
        response = client.get(f"{self._route}/{self.id}/report")
        file_name = response.headers["content-disposition"].split("filename=")[1].replace('"', "")
        output_path = output_path or Path(f"./{file_name}")
        output_path = Path(output_path)

        if output_path.exists():
            raise FileExistsError(f"{output_path} already exists.")

        with open(output_path, "wb") as file_obj:
            file_obj.write(response.content)
        return output_path

    @typechecked
    def get_recommendation(self) -> FeatureJobSetting:
        """
        Retrieve recommended feature job setting from the analysis

        Returns
        -------
        FeatureJobSetting
            Recommended feature job setting
        """
        info = self.info()
        return FeatureJobSetting(**info["recommendation"])

    @typechecked
    def backtest(self, feature_job_setting: FeatureJobSetting) -> pd.DataFrame:
        """
        Backtest using specified feature job setting

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            FeatureJobSetting to backtest

        Returns
        -------
        pd.DataFrame
        """
        payload = FeatureJobSettingAnalysisBacktest(
            feature_job_setting_analysis_id=self.id,
            frequency=feature_job_setting.frequency_seconds,
            job_time_modulo_frequency=feature_job_setting.time_modulo_frequency_seconds,
            blind_spot=feature_job_setting.blind_spot_seconds,
        )
        backtest_results = self.post_async_task(
            route=f"{self._route}/backtest", payload=payload.json_dict(), retrieve_result=False
        )
        output_url = backtest_results["output_url"]

        client = Configurations().get_client()

        # download and display report
        response = client.get(f"{output_url}.html")
        display_html_in_notebook(response.text)

        # download and return data
        response = client.get(f"{output_url}.parquet")
        return pd.read_parquet(path=BytesIO(response.content))

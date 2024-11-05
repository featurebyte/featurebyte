"""
FeatureJobSettingAnalysis class
"""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Union

import pandas as pd
from bson import ObjectId
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisListHandler,
)
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.base_table import TableApiObject
from featurebyte.api.savable_api_object import DeletableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.env_util import display_html_in_notebook
from featurebyte.config import Configurations
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisRecord,
)


class FeatureJobSettingAnalysis(FeatureJobSettingAnalysisModel, DeletableApiObject):
    """
    The FeatureJobSettingAnalysis object contains the result of the analysis of the data availability and freshness of
    a table. The metadata held by the object includes a report and recommendation for the configuration of the feature
    job setting of features associated with the table. Additionally, you can perform a backtest of a manually
    configured feature job setting.

    Examples
    --------
    >>> analysis = invoice_table.create_new_feature_job_setting_analysis(  # doctest: +SKIP
    ...     analysis_date=pd.Timestamp("2023-04-10"),
    ...     analysis_length=3600 * 24 * 28,
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.FeatureJobSettingAnalysis"
    )
    _route: ClassVar[str] = "/feature_job_setting_analysis"
    _list_schema: ClassVar[Any] = FeatureJobSettingAnalysisRecord
    _get_schema: ClassVar[Any] = FeatureJobSettingAnalysisModel
    _list_fields: ClassVar[List[str]] = [
        "created_at",
        "event_table",
        "analysis_start",
        "analysis_date",
        "period",
        "offset",
        "blind_spot",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("event_table_id", TableApiObject, "event_table"),
    ]

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return FeatureJobSettingAnalysisListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = True,
        event_table_id: Optional[ObjectId] = None,
    ) -> pd.DataFrame:
        """
        List saved feature job setting analysis

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        event_table_id: Optional[ObjectId]
            Event table id used to filter results

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        params = {}
        if event_table_id:
            params = {"event_table_id": str(event_table_id)}
        return cls._list(include_id=include_id, params=params)

    @typechecked
    def display_report(self) -> None:
        """
        Displays analysis report.

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> analysis.display_report()  # doctest: +SKIP
        """
        display_html_in_notebook(self.analysis_report)

    @typechecked
    def download_report(
        self, output_path: Optional[Union[str, Path]] = None, overwrite: bool = False
    ) -> Path:
        """
        Downloads analysis report.

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded report
        overwrite: bool
            Overwrite the file if it already exists

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> analysis.download_report()  # doctest: +SKIP
        """
        client = Configurations().get_client()
        response = client.get(f"{self._route}/{self.id}/report")
        file_name = response.headers["content-disposition"].split("filename=")[1].replace('"', "")
        output_path = output_path or Path(f"./{file_name}")
        output_path = Path(output_path)

        if output_path.exists() and not overwrite:
            raise FileExistsError(f"{output_path} already exists.")

        with open(output_path, "wb") as file_obj:
            file_obj.write(response.content)
        return output_path

    @typechecked
    def get_recommendation(self) -> FeatureJobSetting:
        """
        Retrieves recommended feature job setting from the analysis.

        Returns
        -------
        FeatureJobSetting
            Recommended feature job setting

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> feature_job_setting = analysis.get_recommendation()  # doctest: +SKIP
        """
        info = self.info()
        return FeatureJobSetting(**info["recommendation"])

    @typechecked
    def backtest(self, feature_job_setting: FeatureJobSetting) -> pd.DataFrame:
        """
        Backtest using specified feature job setting.

        Parameters
        ----------
        feature_job_setting: FeatureJobSetting
            FeatureJobSetting to backtest

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> # Backtest a manual setting
        >>> manual_setting = fb.FeatureJobSetting(  # doctest: +SKIP
        ...     blind_spot="135s",
        ...     period="60m",
        ...     offset="90s",
        ... )
        >>> backtest_result = analysis.backtest(
        ...     feature_job_setting=manual_setting
        ... )  # doctest: +SKIP
        """
        payload = FeatureJobSettingAnalysisBacktest(
            feature_job_setting_analysis_id=self.id,
            period=feature_job_setting.period_seconds,
            offset=feature_job_setting.offset_seconds,
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

        # download and return table
        response = client.get(f"{output_url}.parquet")
        return pd.read_parquet(path=BytesIO(response.content))

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        The info method provides comprehensive details about a FeatureJobSettingAnalysis object, which encompasses:

        - the creation time of the analysis,
        - the table analyzed,
        - the analysis configuration,
        - recommended feature job setting, and
        - the catalog where the analysis is stored.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> analysis.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    @classmethod
    def get_by_id(
        cls,
        id: ObjectId,
    ) -> FeatureJobSettingAnalysis:
        """
        Retrieves an analysis of the data availability and freshness of a table. This returns a
        FeatureJobSettingAnalysis object that allows to access the result of the analysis.

        Parameters
        ----------
        id: ObjectId
            Analysis unique identifier ID.

        Returns
        -------
        FeatureJobSettingAnalysis
            FeatureJobSettingAnalysis object.

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        """
        return cls._get_by_id(id=id)

    def delete(self) -> None:
        """
        Delete the feature job setting analysis from the persistent data store.

        Examples
        --------
        >>> analysis = fb.FeatureJobSettingAnalysis.get_by_id(<analysis_id>)  # doctest: +SKIP
        >>> analysis.delete()  # doctest: +SKIP
        """
        self._delete()

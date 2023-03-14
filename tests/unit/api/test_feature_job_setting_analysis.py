"""
Unit test for FeatureJobSettingAnalysis class
"""
import json
import os
import tempfile
from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytest
import pytest_asyncio
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.api.feature_job_setting_analysis import FeatureJobSettingAnalysis
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from tests.util.helper import patch_import_package


@pytest_asyncio.fixture(name="saved_analysis")
async def saved_analysis_fixture(mock_get_persistent, saved_event_data):
    """
    Saved analysis
    """
    persistent = mock_get_persistent()
    _ = saved_event_data
    with open("tests/fixtures/feature_job_setting_analysis/result.json", "r") as file_handle:
        analysis = FeatureJobSettingAnalysisModel(**json.load(file_handle))
    analysis.event_data_id = saved_event_data.id
    return await persistent.insert_one(
        collection_name=FeatureJobSettingAnalysisModel.collection_name(),
        document=analysis.dict(),
        user_id=None,
    )


def test_list(saved_analysis, saved_event_data):
    """
    Test list analysis
    """
    analysis_id = saved_analysis
    result = FeatureJobSettingAnalysis.list()
    assert result.shape == (1, 8)
    assert result.columns.to_list() == [
        "id",
        "created_at",
        "event_data",
        "analysis_start",
        "analysis_date",
        "frequency",
        "job_time_modulo_frequency",
        "blind_spot",
    ]
    assert result["id"].iloc[0] == analysis_id
    assert result["event_data"].iloc[0] == "sf_event_data"

    # list with filter
    assert FeatureJobSettingAnalysis.list(event_data_id=saved_event_data.id).shape == (1, 8)
    assert FeatureJobSettingAnalysis.list(event_data_id=ObjectId()).shape == (0, 8)


@patch("featurebyte.common.env_util.is_notebook")
def test_display_report(mock_is_notebook, saved_analysis):
    """
    Test display_report
    """
    analysis = FeatureJobSettingAnalysis.get_by_id(saved_analysis)
    with patch_import_package("IPython.display") as mock_mod:
        mock_is_notebook.return_value = False
        analysis.display_report()
        # check that ipython display not get called
        assert mock_mod.display.call_count == 0
        assert mock_mod.HTML.call_count == 0

    with patch_import_package("IPython.display") as mock_mod:
        mock_is_notebook.return_value = True
        analysis.display_report()

        # check that ipython display get called
        assert mock_mod.display.call_count == 1
        assert mock_mod.HTML.call_count == 1


def test_info(saved_analysis):
    """
    Test info
    """
    analysis = FeatureJobSettingAnalysis.get_by_id(saved_analysis)
    assert analysis.info() == {
        "created_at": analysis.created_at.isoformat(),
        "event_data_name": "sf_event_data",
        "analysis_options": {
            "analysis_date": "2022-04-18T23:59:55.799000",
            "analysis_start": "2022-03-21T23:59:55.799000",
            "analysis_length": 2419200,
            "blind_spot_buffer_setting": 5,
            "exclude_late_job": False,
            "job_time_buffer_setting": "auto",
            "late_data_allowance": 5e-05,
            "min_featurejob_period": 60,
        },
        "analysis_parameters": {
            "event_data_name": "sf_event_data",
            "creation_date_column": "created_at",
            "event_timestamp_column": "event_timestamp",
            "blind_spot_buffer": 5,
            "job_time_buffer": 5,
            "frequency": 180,
            "granularity": 5,
            "reading_at": 1,
            "job_time_modulo_frequency": 61,
        },
        "recommendation": {
            "blind_spot": "395s",
            "frequency": "180s",
            "time_modulo_frequency": "61s",
        },
        "workspace_name": "default",
    }


def test_get_recommendation(saved_analysis):
    """
    Test get_recommendation
    """
    analysis = FeatureJobSettingAnalysis.get_by_id(saved_analysis)
    assert analysis.get_recommendation() == FeatureJobSetting(
        blind_spot="395s",
        frequency="180s",
        time_modulo_frequency="61s",
    )


@patch("featurebyte.api.feature_job_setting_analysis.FeatureJobSettingAnalysis.post_async_task")
@patch("featurebyte.api.feature_job_setting_analysis.display_html_in_notebook")
def test_backtest(mock_display_html, mock_post_async_task, saved_analysis):
    """
    Test backtest
    """
    analysis = FeatureJobSettingAnalysis.get_by_id(saved_analysis)
    analysis.info()
    output_url = "/temp_data?path=feature_job_setting_analysis/backtest/63c64fddca8203076aa33461"
    mock_post_async_task.return_value = {"output_url": output_url}
    expected_results = pd.DataFrame({"value": [1, 2, 3]})
    buffer = BytesIO()
    expected_results.to_parquet(buffer)

    with patch("featurebyte.config.Configurations.get_client") as mock_get_client:
        client = mock_get_client.return_value
        client.get.return_value.status_code = 200
        client.get.return_value.text = "html report content"
        client.get.return_value.content = buffer.getbuffer()
        backtest_result = analysis.backtest(
            FeatureJobSetting(
                blind_spot="50s",
                frequency="2m",
                time_modulo_frequency="100s",
            )
        )

    mock_post_async_task.assert_called_once_with(
        route="/feature_job_setting_analysis/backtest",
        payload={
            "_id": mock_post_async_task.call_args[1]["payload"]["_id"],
            "feature_job_setting_analysis_id": str(analysis.id),
            "frequency": 120,
            "job_time_modulo_frequency": 100,
            "blind_spot": 50,
        },
        retrieve_result=False,
    )
    assert client.get.call_count == 2
    mock_display_html.assert_called_once_with("html report content")
    assert_frame_equal(backtest_result, expected_results)


def test_download_report(saved_analysis):
    """
    Test download_report
    """
    analysis = FeatureJobSettingAnalysis.get_by_id(saved_analysis)
    with tempfile.NamedTemporaryFile() as file_obj:
        with pytest.raises(FileExistsError) as exc:
            analysis.download_report(output_path=file_obj.name)
            assert str(exc) == f"{file_obj.name} already exists"

        # download should work if path does not exist
        os.unlink(file_obj.name)
        analysis.download_report(output_path=file_obj.name)

"""
Test data description task
"""

import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.common.utils import dataframe_from_json
from featurebyte.models import FeatureStoreModel
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.schema.worker.task.data_description import DataDescriptionTaskPayload
from featurebyte.worker.task.data_description import DataDescriptionTask
from tests.unit.worker.task.base import BaseTaskTestSuite


class TestDataDescriptionTask(BaseTaskTestSuite):
    """
    Test suite for Feature Job Setting Analysis worker task
    """

    task_class = DataDescriptionTask
    payload = DataDescriptionTaskPayload(
        sample=BaseTaskTestSuite.load_payload(
            "tests/fixtures/request_payloads/feature_store_sample.json"
        ),
        size=0,
        seed=1234,
        catalog_id=ObjectId(),
    ).json_dict()

    async def setup_persistent_storage(self, persistent, storage, temp_storage, catalog):
        """
        Setup for post route
        """
        # update payload to make checking easier
        self.payload["sample"]["graph"]["nodes"][0]["parameters"]["columns"] = [
            "col_float",
            "event_timestamp",
        ]

        # save feature store if it doesn't already exist
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        try:
            await persistent.insert_one(
                collection_name=FeatureStoreModel.collection_name(),
                document=FeatureStoreModel(**payload).model_dump(by_alias=True),
                user_id=None,
            )
        except DuplicateDocumentError:
            # do nothing as it means this has been created before
            pass

    @pytest.fixture(autouse=True)
    def patch_execute_query(self):
        """
        Patch execute_query
        """
        with patch(
            "featurebyte.session.base.BaseSession.execute_query_long_running"
        ) as mock_execute_query:
            mock_execute_query.return_value = pd.DataFrame(
                {
                    "a_dtype": ["FLOAT"],
                    "a_unique": [5],
                    "a_%missing": [1.0],
                    "a_%empty": [np.nan],
                    "a_entropy": [np.nan],
                    "a_top": [np.nan],
                    "a_freq": [np.nan],
                    "a_mean": [0.256],
                    "a_std": [0.00123],
                    "a_min": [0],
                    "a_p25": [0.01],
                    "a_p50": [0.155],
                    "a_p75": [0.357],
                    "a_max": [1.327],
                    "a_min_offset": [np.nan],
                    "a_max_offset": [np.nan],
                    "b_dtype": ["VARCHAR"],
                    "b_unique": [5],
                    "b_%missing": [3.0],
                    "b_%empty": [0.1],
                    "b_entropy": [0.123],
                    "b_top": ["a"],
                    "b_freq": [5],
                    "b_mean": [np.nan],
                    "b_std": [np.nan],
                    "b_min": [np.nan],
                    "b_p25": [np.nan],
                    "b_p50": [np.nan],
                    "b_p75": [np.nan],
                    "b_max": [np.nan],
                    "b_min_offset": [np.nan],
                    "b_max_offset": [np.nan],
                }
            )
            yield mock_execute_query

    @pytest.fixture(autouse=True)
    def insert_credential_fixture(self, insert_credential):
        """
        Insert default credential into db.
        """
        _ = insert_credential
        yield

    @pytest.mark.asyncio
    async def test_execute_success(  # pylint: disable=too-many-locals
        self, task_completed, temp_storage
    ):
        """
        Test successful task execution
        """
        _ = task_completed
        output_document_id = self.payload["output_document_id"]
        payload = DataDescriptionTaskPayload(**self.payload)
        assert (
            payload.task_output_path
            == f"/temp_data?path=data_description/{output_document_id}.json"
        )

        result = await temp_storage.get_text(f"data_description/{output_document_id}.json")
        expected_df = pd.DataFrame(
            {
                "col_float": [
                    "FLOAT",
                    5,
                    1.0,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    0.256,
                    0.00123,
                    0,
                    0.01,
                    0.155,
                    0.357,
                    1.327,
                ],
                "event_timestamp": [
                    "VARCHAR",
                    5,
                    3.0,
                    0.1,
                    0.123,
                    "a",
                    5,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                ],
            },
            index=[
                "dtype",
                "unique",
                "%missing",
                "%empty",
                "entropy",
                "top",
                "freq",
                "mean",
                "std",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            ],
        )
        assert_frame_equal(dataframe_from_json(json.loads(result)), expected_df, check_dtype=False)

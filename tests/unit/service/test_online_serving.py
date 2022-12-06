"""
Tests for OnlineServingService
"""
import textwrap
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from featurebyte.exception import FeatureListNotOnlineEnabledError


@pytest.fixture
def entity_serving_names():
    """
    Fixture for entity serving names for requesting online features
    """
    return [{"cust_id": 1}]


@pytest.mark.asyncio
async def test_feature_list_not_deployed(
    online_serving_service,
    feature_list,
    entity_serving_names,
):
    """
    Test getting online features for not yet deployed feature list is not allowed
    """
    with pytest.raises(FeatureListNotOnlineEnabledError) as exc:
        await online_serving_service.get_online_features_from_feature_list(
            feature_list=feature_list,
            entity_serving_names=entity_serving_names,
            get_credential=Mock(),
        )
    assert str(exc.value) == "Feature List is not online enabled"


@pytest.mark.asyncio
async def test_feature_list_deployed(
    online_serving_service,
    deployed_feature_list,
    entity_serving_names,
):
    """
    Test getting online features request for a valid feature list
    """

    async def mock_execute_query(query):
        _ = query
        return pd.DataFrame({"cust_id": [1], "feature_value": [123.0]})

    mock_session = Mock(
        name="mock_session_for_online_serving", execute_query=Mock(side_effect=mock_execute_query)
    )
    with patch(
        "featurebyte.service.online_serving.SessionManagerService.get_feature_store_session"
    ) as mock_get_feature_store_session:
        mock_get_feature_store_session.return_value = mock_session
        result = await online_serving_service.get_online_features_from_feature_list(
            feature_list=deployed_feature_list,
            entity_serving_names=entity_serving_names,
            get_credential=Mock(),
        )

    # Check result
    assert result.dict() == {"features": [{"cust_id": 1.0, "feature_value": 123.0}]}

    # Check query used
    assert len(mock_session.execute_query.call_args_list) == 1
    args, _ = mock_session.execute_query.call_args
    assert (
        args[0]
        == textwrap.dedent(
            """
            SELECT
              REQ."cust_id",
              T0."sum_30m"
            FROM (
              SELECT
                1 AS "cust_id"
            ) AS REQ
            LEFT JOIN online_store_d2ed607faa7d8527306806fca080c333051df742 AS T0
              ON REQ."cust_id" = T0."cust_id"
            """
        ).strip()
    )

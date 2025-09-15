"""
Helpers for testing deployment
"""

from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import patch

import pandas as pd

from featurebyte import FeatureList
from featurebyte.schema.feature_list import OnlineFeaturesRequestPayload


def deploy_and_get_online_features(
    client: Any,
    feature_list: FeatureList,
    deploy_at: datetime,
    request_data: List[Dict[str, Any]],
) -> pd.DataFrame:
    """
    Deploy and get online features for a feature list in integration tests
    """
    if not feature_list.saved:
        feature_list.save()

    deployment = feature_list.deploy(make_production_ready=True)
    try:
        with (
            patch(
                "featurebyte.service.feature_manager.get_next_job_datetime",
                return_value=deploy_at,
            ),
            patch(
                "featurebyte.service.feature_materialize.datetime", autospec=True
            ) as mock_datetime,
        ):
            mock_datetime.utcnow.return_value = deploy_at
            deployment.enable()

        data = OnlineFeaturesRequestPayload(entity_serving_names=request_data)
        with patch("featurebyte.service.online_serving.datetime", autospec=True) as mock_datetime:
            with patch(
                "croniter.croniter.get_prev"
            ) as mock_get_prev:  # for cron feature job settings
                mock_datetime.utcnow.return_value = deploy_at
                mock_get_prev.return_value = deploy_at.timestamp()
                res = client.post(
                    f"/deployment/{deployment.id}/online_features",
                    json=data.json_dict(),
                )
        assert res.status_code == 200
        df_feat = pd.DataFrame(res.json()["features"])
    finally:
        if deployment.enabled:
            deployment.disable()
    return df_feat

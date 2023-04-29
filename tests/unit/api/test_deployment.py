"""
Unit tests for Deployment class
"""
import textwrap
from unittest.mock import patch

import pandas as pd
import pytest

import featurebyte as fb
from featurebyte.api.deployment import Deployment
from featurebyte.config import Configurations
from featurebyte.exception import FeatureListNotOnlineEnabledError


def test_get(deployment):
    """
    Test retrieving a Deployment object by name
    """
    retrieved_deployment = Deployment.get(deployment.name)
    assert retrieved_deployment == deployment


def test_list(deployment):
    """
    Test listing Deployment objects
    """
    df = deployment.list()
    expected = pd.DataFrame(
        [
            {
                "id": deployment.id,
                "name": deployment.name,
                "feature_list_name": "my_feature_list",
                "feature_list_version": f'V{pd.Timestamp.now().strftime("%y%m%d")}',
                "num_feature": 1,
            }
        ]
    )
    pd.testing.assert_frame_equal(df, expected[df.columns.tolist()])


def test_info(deployment):
    """Test get deployment info"""
    info_dict = deployment.info()
    expected_version = f'V{pd.Timestamp.now().strftime("%y%m%d")}'
    assert info_dict == {
        "name": f"Deployment with my_feature_list_{expected_version}",
        "feature_list_name": "my_feature_list",
        "feature_list_version": expected_version,
        "num_feature": 1,
        "enabled": False,
        "serving_endpoint": None,
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }


def test_get_online_serving_code_not_deployed(deployment):
    """Test feature get_online_serving_code on un-deployed feature list"""
    with pytest.raises(FeatureListNotOnlineEnabledError) as exc:
        deployment.get_online_serving_code()
    assert "Deployment is not enabled." in str(exc.value)


def test_get_online_serving_code_unsupported_language(deployment):
    """Test feature get_online_serving_code with unsupported language"""
    deployment.enable()
    assert deployment.enabled is True
    with pytest.raises(NotImplementedError) as exc:
        deployment.get_online_serving_code(language="java")
    assert "Supported languages: ['python', 'sh']" in str(exc.value)


def test_list_deployment(deployment):
    """
    Test summarizing Deployment objects
    """
    config = Configurations()
    client = config.get_client()

    # enable deployment
    deployment.enable()

    fb.Catalog.get_active()
    response = client.get("/deployment/summary/")
    assert response.status_code == 200
    assert response.json() == {"num_feature_list": 1, "num_feature": 1}

    # make sure deployment can be retrieved in different catalog
    catalog = fb.Catalog.create("another_catalog")
    fb.Catalog.activate(catalog.name)
    response = client.get("/deployment/summary/")
    assert response.json() == {"num_feature_list": 1, "num_feature": 1}


@patch("featurebyte.core.mixin.SampleMixin.preview")
def test_get_online_serving_code(mock_preview, deployment):
    """Test feature get_online_serving_code"""
    mock_preview.return_value = pd.DataFrame(
        {"col_int": ["sample_col_int"], "cust_id": ["sample_cust_id"]}
    )
    deployment.enable()
    assert deployment.enabled is True
    assert (
        deployment.get_online_serving_code().strip()
        == textwrap.dedent(
            f'''
            from typing import Any, Dict

            import pandas as pd
            import requests


            def request_features(entity_serving_names: Dict[str, Any]) -> pd.DataFrame:
                """
                Send POST request to online serving endpoint

                Parameters
                ----------
                entity_serving_names: Dict[str, Any]
                    Entity serving name values to used for serving request

                Returns
                -------
                pd.DataFrame
                """
                response = requests.post(
                    url="http://localhost:8080/deployment/{deployment.id}/online_features",
                    params={{"catalog_id": "63eda344d0313fb925f7883a"}},
                    headers={{"Content-Type": "application/json", "Authorization": "Bearer token"}},
                    json={{"entity_serving_names": entity_serving_names}},
                )
                assert response.status_code == 200, response.json()
                return pd.DataFrame.from_dict(response.json()["features"])


            request_features([{{"cust_id": "sample_cust_id"}}])
            '''
        ).strip()
    )
    url = (
        f"http://localhost:8080/deployment/{deployment.id}/online_features"
        f"?catalog_id={deployment.cached_model.catalog_id}"
    )
    assert (
        deployment.get_online_serving_code(language="sh").strip()
        == textwrap.dedent(
            f"""
            #!/bin/sh

            curl -X POST -H 'Content-Type: application/json' -H 'Authorization: Bearer token' -d \\
                '{{"entity_serving_names": [{{"cust_id": "sample_cust_id"}}]}}' \\
                {url}
            """
        ).strip()
    )

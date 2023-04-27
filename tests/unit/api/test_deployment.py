"""
Unit tests for Deployment class
"""
import pandas as pd

from featurebyte.api.deployment import Deployment


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
                "catalog": "default",
            }
        ]
    )
    pd.testing.assert_frame_equal(df, expected[df.columns.tolist()])


def test_info(deployment):
    """Test get deployment info"""
    info_dict = deployment.info()
    expected_version = f'V{pd.Timestamp.now().strftime("%y%m%d")}'
    assert info_dict == {
        "name": f'Deployment (feature_list: "my_feature_list", version: {expected_version})',
        "feature_list_name": "my_feature_list",
        "feature_list_version": expected_version,
        "num_feature": 1,
        "enabled": False,
        "created_at": info_dict["created_at"],
        "updated_at": None,
    }

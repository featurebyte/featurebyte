"""
Test for VersionService
"""
import pytest

from featurebyte.common.model_util import get_version
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.schema.feature import VersionCreate


@pytest.mark.asyncio
async def test_create_new_feature_version(
    version_service, feature, feature_namespace_service, get_credential
):
    """Test create new feature version"""
    version = await version_service.create_new_feature_version(
        data=VersionCreate(
            source_feature_id=feature.id,
            feature_job_setting=FeatureJobSetting(
                blind_spot="1d", frequency="1d", time_modulo_frequency="1h"
            ),
        ),
        get_credential=get_credential,
    )

    # compare groupby node
    expected_common_params = {
        "keys": ["cust_id"],
        "parent": "col_float",
        "agg_func": "sum",
        "value_by": None,
        "windows": ["30m", "2h", "1d"],
        "timestamp": "event_timestamp",
        "names": ["sum_30m", "sum_2h", "sum_1d"],
        "serving_names": ["cust_id"],
    }
    parameters = feature.graph.get_node_by_name("groupby_1").parameters
    assert parameters.dict() == {
        **expected_common_params,
        "blind_spot": 600,
        "time_modulo_frequency": 300,
        "frequency": 1800,
        "tile_id": "sf_table_f1800_m300_b600_f3822df3690ac033f56672194a2f224586d0a5bd",
        "aggregation_id": "sum_afb4d56e30a685ee9128bfa58fe4ad76d32af512",
    }

    new_parameters = version.graph.get_node_by_name("groupby_1").parameters
    assert new_parameters.dict() == {
        **expected_common_params,
        "blind_spot": 86400,
        "time_modulo_frequency": 3600,
        "frequency": 86400,
        "tile_id": "sf_table_f86400_m3600_b86400_d1c26f05670a559eec7e90107400f228b14c209f",
        "aggregation_id": "sum_27c04cd4b7f10ab112b95b39b21f294d353752f7",
    }

    # compare edges & other nodes
    assert feature.graph.edges == version.graph.edges
    for node_name in ["input_1", "project_1"]:
        assert feature.graph.get_node_by_name(node_name) == version.graph.get_node_by_name(
            node_name
        )
    _ = feature

    # check version
    version_name = get_version()
    assert feature.version == {"name": version_name, "suffix": None}
    assert version.version == {"name": version_name, "suffix": 1}

    # check other attributes
    assert version.node_name == "project_1"
    assert version.readiness == "DRAFT"
    assert version.feature_namespace_id == feature.feature_namespace_id
    assert version.entity_ids == feature.entity_ids
    assert version.feature_list_ids == []
    assert version.deployed_feature_list_ids == []
    assert version.online_enabled is False

    # check feature namespace service get updated
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.feature_ids == [feature.id, version.id]

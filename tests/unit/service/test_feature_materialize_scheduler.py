"""
Teat FeatureMaterializeSchedulerService
"""
import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.periodic_task import Interval
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.graph import QueryGraph


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def fixture_offline_store_feature_table(app_container, feature_job_setting):
    """
    Fixture for offline store feature table
    """
    feature_table = OfflineStoreFeatureTableModel(
        name="my_feature_table",
        feature_ids=[ObjectId()],
        primary_entity_ids=[ObjectId()],
        serving_names=["cust_id"],
        feature_job_setting=feature_job_setting,
        has_ttl=True,
        feature_cluster=FeatureCluster(
            graph=QueryGraph(), node_names=[], feature_store_id=ObjectId()
        ),
        output_column_names=["col1", "col2"],
        output_dtypes=["VARCHAR", "FLOAT"],
        entity_universe=EntityUniverseModel(
            query_template=SqlglotExpressionModel(
                formatted_expression="SELECT DISTINCT cust_id FROM my_table",
            )
        ),
    )
    await app_container.offline_store_feature_table_service.create_document(feature_table)
    yield feature_table


@pytest.fixture(name="service")
def service_fixture(app_container):
    """
    Fixture for FeatureMaterializeSchedulerService
    """
    return app_container.feature_materialize_scheduler_service


@pytest.mark.parametrize(
    "feature_job_setting",
    [
        FeatureJobSetting(frequency="1h", blind_spot="0s", time_modulo_frequency="5m"),
        FeatureJobSetting(frequency="1d", blind_spot="0s", time_modulo_frequency="5m"),
    ],
)
@pytest.mark.asyncio
async def test_start_and_stop_job(service, feature_job_setting, offline_store_feature_table):
    """
    Test start_job with different feature job settings
    """
    # Test starting job
    await service.start_job_if_not_exist(offline_store_feature_table)
    periodic_task = await service.get_periodic_task(offline_store_feature_table.id)
    assert periodic_task is not None
    assert periodic_task.interval == Interval(
        every=offline_store_feature_table.feature_job_setting.frequency_seconds, period="seconds"
    )
    assert (
        periodic_task.time_modulo_frequency_second
        == feature_job_setting.time_modulo_frequency_seconds
    )
    assert periodic_task.kwargs["offline_store_feature_table_id"] == str(
        offline_store_feature_table.id
    )
    assert (
        periodic_task.kwargs["offline_store_feature_table_name"] == offline_store_feature_table.name
    )

    # Test stopping job
    await service.stop_job(offline_store_feature_table.id)
    periodic_task = await service.get_periodic_task(offline_store_feature_table.id)
    assert periodic_task is None

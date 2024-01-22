"""
Test OfflineStoreFeatureTableService
"""
import pytest
from bson import ObjectId

from featurebyte import FeatureJobSetting
from featurebyte.models.base_feature_or_target_table import FeatureCluster
from featurebyte.models.entity_universe import EntityUniverseModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.sqlglot_expression import SqlglotExpressionModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.routes.registry import app_container_config


@pytest.fixture(name="service")
def fixture_service(app_container):
    """
    Fixture for OfflineStoreFeatureTableService
    """
    return app_container.offline_store_feature_table_service


@pytest.fixture(name="common_params")
def fixture_common_params(catalog):
    """Common parameters for OfflineStoreFeatureTable"""
    return {
        "catalog_id": catalog.id,
        "feature_ids": [],
        "primary_entity_ids": [],
        "serving_names": ["cust_id"],
        "feature_job_setting": FeatureJobSetting(
            frequency="1d", time_modulo_frequency="1h", blind_spot="1d"
        ),
        "has_ttl": False,
        "last_materialized_at": None,
        "feature_cluster": FeatureCluster(
            feature_store_id=ObjectId(),
            graph=QueryGraph(),
            node_names=[],
        ),
        "output_column_names": [],
        "output_dtypes": [],
        "entity_universe": EntityUniverseModel(
            query_template=SqlglotExpressionModel(formatted_expression="")
        ),
        "feature_store_id": ObjectId(),
    }


@pytest.fixture(name="another_catalog_id")
def fixture_another_catalog_id():
    """Fixture for another catalog id"""
    return ObjectId("65a66b90afb61f8eddd27244")  # only differ by last digit


@pytest.fixture(name="another_service")
def fixture_another_service(app_container, another_catalog_id):
    """
    Fixture for OfflineStoreFeatureTableService
    """
    app_container = LazyAppContainer(
        app_container_config=app_container_config,
        instance_map={
            "user": app_container.user,
            "persistent": app_container.persistent,
            "catalog_id": another_catalog_id,
        },
    )
    return app_container.offline_store_feature_table_service


@pytest.mark.asyncio
async def test_create_document_and_retrieve_document(
    service,
    common_params,
    another_catalog_id,
    another_service,
):
    """Test create_document"""
    data = OfflineStoreFeatureTableModel(**common_params)
    document = await service.create_document(data=data)
    assert document.name_prefix == "cat1"
    assert document.name == "cust_id_1d"
    assert document.name_suffix is None
    assert document.name == "cat1_cust_id_1d"

    # create another document with same name
    data = OfflineStoreFeatureTableModel(**{**common_params, "_id": ObjectId()})
    document = await service.create_document(data=data)
    assert document.name_prefix == "cat1"
    assert document.name == "cust_id_1d"
    assert document.name_suffix == "1"
    assert document.name == "cat1_cust_id_1d_1"

    # create another document in another catalog (expect no name conflict, full name should be different)
    data = OfflineStoreFeatureTableModel(**{**common_params, "catalog_id": another_catalog_id})
    document = await another_service.create_document(data=data)
    assert document.name_prefix == "cat2"
    assert document.name == "cust_id_1d"
    assert document.name_suffix is None
    assert document.name == "cat2_cust_id_1d"
    assert document.catalog_id == another_catalog_id

    # create another document with different feature_store_id (same name is allowed)
    another_feature_store_id = ObjectId()
    data = OfflineStoreFeatureTableModel(
        **{**common_params, "feature_store_id": another_feature_store_id}
    )
    document = await service.create_document(data=data)
    assert document.name_prefix == "cat1"
    assert document.name == "cust_id_1d"
    assert document.name_suffix is None
    assert document.name == "cat1_cust_id_1d"

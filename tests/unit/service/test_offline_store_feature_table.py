"""
Unit Tests for OfflineStoreFeatureTableService
"""

import os
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson import ObjectId, json_util

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.feature_list import FeatureCluster
from featurebyte.models.offline_store_feature_table import (
    FeaturesUpdate,
    OfflineStoreFeatureTableModel,
)


@pytest.fixture(name="service")
def offline_store_feature_table_service_fixture(app_container):
    """Fixture for OfflineStoreFeatureTableService"""
    return app_container.offline_store_feature_table_service


@pytest.fixture(name="offline_store_feature_table_dict")
def offline_store_feature_table_dict_fixture(test_dir, service, user_id):
    """Fixture for OfflineStoreFeatureTableModel"""
    path = os.path.join(
        test_dir, "fixtures/offline_store_feature_table/feature_cluster_one_feature.json"
    )
    with open(path, "r") as file:
        feature_cluster = FeatureCluster(**json_util.loads(file.read()))

    return {
        "catalog_id": service.catalog_id,
        "entity_universe": {"query_template": {"formatted_expression": ""}},
        "feature_ids": [ObjectId()],
        "feature_cluster": feature_cluster,
        "feature_job_setting": {"blind_spot": "600s", "period": "1800s", "offset": "300s"},
        "has_ttl": True,
        "entity_lookup_info": None,
        "last_materialized_at": None,
        "name": "cat1_cust_id_30m",
        "name_prefix": "cat1",
        "name_suffix": None,
        "online_stores_last_materialized_at": [],
        "output_column_names": ["sum_1d_V231227", "sum_1d_plus_123_V231227"],
        "output_dtypes": ["FLOAT", "FLOAT"],
        "primary_entity_ids": [],
        "serving_names": ["cust_id"],
        "user_id": user_id,
        "aggregation_ids": ["agg_id_1"],
    }


@pytest.fixture(name="feature_cluster_two_features")
def feature_cluster_two_features_fixture(test_dir):
    """Fixture for feature_cluster_two_features"""
    path = os.path.join(
        test_dir, "fixtures/offline_store_feature_table/feature_cluster_two_features.json"
    )
    with open(path, "r") as file:
        return FeatureCluster(**json_util.loads(file.read()))


@pytest_asyncio.fixture(name="offline_store_feature_table")
async def offline_store_feature_table_fixture(service, offline_store_feature_table_dict):
    """Fixture for OfflineStoreFeatureTableModel"""
    data = OfflineStoreFeatureTableModel(**offline_store_feature_table_dict)
    assert data.feature_cluster_path is None
    table = await service.create_document(data=data)
    return table


@pytest.mark.asyncio
async def test_create_multiple_documents(service, offline_store_feature_table_dict):
    """Test create multiple documents"""
    tables = []
    for _ in range(3):
        data = OfflineStoreFeatureTableModel(**offline_store_feature_table_dict)
        tables.append(await service.create_document(data=data))

    table_names = [table.name for table in tables]
    table_base_names = {table.base_name for table in tables}
    assert table_names == [
        "cat1_cust_id_30m",
        "cat1_cust_id_30m_1",
        "cat1_cust_id_30m_2",
    ]
    assert table_base_names == {"cust_id_30m"}


@pytest.mark.asyncio
async def test_create_document(service, offline_store_feature_table, storage):
    """Test create_document"""
    # check the feature cluster path is set
    assert offline_store_feature_table.feature_cluster_path is not None
    assert offline_store_feature_table.feature_cluster_path == (
        f"catalog/{service.catalog_id}/"
        f"offline_store_feature_table/{offline_store_feature_table.id}/feature_cluster.json"
    )

    # check the feature cluster file exists
    cluster_path = os.path.join(storage.base_path, offline_store_feature_table.feature_cluster_path)
    assert os.path.exists(cluster_path)


@pytest.mark.asyncio
async def test_get_document(offline_store_feature_table, service, offline_store_feature_table_dict):
    """Test get_document"""
    table = await service.get_document(offline_store_feature_table.id)
    assert table.id == offline_store_feature_table.id
    assert isinstance(table.feature_cluster, FeatureCluster)

    # check deserialized feature cluster
    assert table.feature_cluster == offline_store_feature_table_dict["feature_cluster"]

    # check the feature cluster is not populated if populate_remote_attributes is False
    table = await service.get_document(
        offline_store_feature_table.id, populate_remote_attributes=False
    )
    assert table.feature_cluster is None


@pytest.mark.asyncio
async def test_list_documents_iterator(offline_store_feature_table, service):
    """Test list_documents_iterator"""
    tables = []
    async for table in service.list_documents_iterator(query_filter={}):
        tables.append(table)

    assert len(tables) == 1
    assert tables[0].id == offline_store_feature_table.id
    assert tables[0].feature_cluster is not None
    assert tables[0].feature_cluster == offline_store_feature_table.feature_cluster

    # check the feature cluster is not populated if populate_remote_attributes is False
    async for table in service.list_documents_iterator(
        query_filter={}, populate_remote_attributes=False
    ):
        assert table.feature_cluster is None


@pytest.mark.asyncio
async def test_delete_document(offline_store_feature_table, service, storage):
    """Test delete_document"""
    await service.delete_document(offline_store_feature_table.id)

    # check the document is deleted
    with pytest.raises(DocumentNotFoundError):
        await service.get_document(offline_store_feature_table.id)

    # check the feature cluster file is deleted
    cluster_path = os.path.join(storage.base_path, offline_store_feature_table.feature_cluster_path)
    assert not os.path.exists(cluster_path)


@pytest.mark.asyncio
async def test_update_document(
    service, offline_store_feature_table, storage, feature_cluster_two_features
):
    """Test update_document"""
    updated_doc = await service.update_document(
        document_id=offline_store_feature_table.id,
        data=FeaturesUpdate(
            feature_ids=[ObjectId(), ObjectId()],
            feature_cluster=feature_cluster_two_features,
            output_column_names=["col1", "col2"],
            output_dtypes=["FLOAT", "FLOAT"],
            entity_universe=offline_store_feature_table.entity_universe,
            aggregation_ids=[],
        ),
        populate_remote_attributes=True,
    )
    assert updated_doc.feature_cluster == feature_cluster_two_features

    # check the feature cluster file is updated
    cluster_path = os.path.join(storage.base_path, updated_doc.feature_cluster_path)
    assert os.path.exists(cluster_path)
    with open(cluster_path, "r") as file:
        cluster = FeatureCluster(**json_util.loads(file.read()))
        assert cluster == feature_cluster_two_features

    updated_doc_dict = await service.get_document_as_dict(updated_doc.id)
    expected_path = (
        f"catalog/{service.catalog_id}/offline_store_feature_table/"
        f"{offline_store_feature_table.id}/feature_cluster.json"
    )
    assert updated_doc_dict["feature_cluster"] is None
    assert updated_doc_dict["feature_cluster_path"] == expected_path


@pytest.mark.asyncio
async def test_update_document__with_failure(
    service, offline_store_feature_table, feature_cluster_two_features
):
    """Test update_document with failure"""
    cluster_path = os.path.join(
        service.storage.base_path, offline_store_feature_table.feature_cluster_path
    )

    update_data = FeaturesUpdate(
        feature_ids=[ObjectId(), ObjectId()],
        feature_cluster=feature_cluster_two_features,
        output_column_names=["col1", "col2"],
        output_dtypes=["FLOAT", "FLOAT"],
        entity_universe=offline_store_feature_table.entity_universe,
        aggregation_ids=[],
    )

    assert os.path.exists(cluster_path)
    with patch.object(service, "_move_feature_cluster_to_storage") as mock_move_storage:
        mock_move_storage.side_effect = Exception("Random error")
        with pytest.raises(Exception, match="Random error"):
            await service.update_document(
                document_id=offline_store_feature_table.id, data=update_data
            )

    # check that feature cluster remote file is deleted
    assert not os.path.exists(cluster_path)

    # update the document again & check the feature cluster file is created
    updated_doc = await service.update_document(
        document_id=offline_store_feature_table.id,
        data=update_data,
        populate_remote_attributes=True,
    )
    assert updated_doc.feature_cluster == feature_cluster_two_features
    cluster_path = os.path.join(service.storage.base_path, updated_doc.feature_cluster_path)
    assert os.path.exists(cluster_path)


@pytest.mark.asyncio
async def test_list_feature_tables_for_aggregation_id(service, offline_store_feature_table):
    """
    Test listing documents given an aggregation_id
    """
    docs = []
    async for doc in service.list_feature_tables_for_aggregation_ids(["agg_id_1"]):
        docs.append(doc)
    assert len(docs) == 1
    assert docs[0].id == offline_store_feature_table.id

    docs = []
    async for doc in service.list_feature_tables_for_aggregation_ids(["agg_id_unknown"]):
        docs.append(doc)
    assert len(docs) == 0

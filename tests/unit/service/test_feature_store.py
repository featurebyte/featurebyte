"""
Test feature store class
"""
import pytest

from featurebyte import SnowflakeDetails, SourceType
from featurebyte.exception import DocumentNotFoundError
from featurebyte.schema.feature_store import FeatureStoreCreate


@pytest.fixture(name="test_snowflake_details")
def get_test_snowflake_details():
    """
    Get test snowflake details
    """
    return SnowflakeDetails(
        account="sf_details_account",
        warehouse="sf_details_warehouse",
        database="sf_details_database",
        sf_schema="sf_details_schema",
    )


@pytest.mark.asyncio
async def test_delete_feature_store(feature_store_service, snowflake_feature_store, get_credential):
    """
    Tests the delete feature store service endpoint.
    """
    data = FeatureStoreCreate(
        name="test_feature_store",
        type=SourceType.SNOWFLAKE,
        details=snowflake_feature_store.details,
    )
    doc_created = await feature_store_service.create_document(
        data=data, get_credential=get_credential
    )
    # Verify that the document exists now
    document_response = await feature_store_service.get_document(doc_created.id)
    assert document_response.id == doc_created.id

    items_deleted = await feature_store_service.delete_feature_store(doc_created.id)
    assert items_deleted == 1
    # Verify that the document doesn't exist
    with pytest.raises(DocumentNotFoundError):
        await feature_store_service.get_document(doc_created.id)

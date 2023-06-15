"""
Test for OnlineStoreTableVersionService
"""
import pytest
import pytest_asyncio

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.online_store_table_version import OnlineStoreTableVersion


@pytest.fixture
def online_store_table_versions():
    """
    Fixture to create OnlineStoreTableVersion models
    """
    return [
        OnlineStoreTableVersion(
            online_store_table_name="store_1",
            aggregation_result_name="result_1",
            version=1,
        ),
        OnlineStoreTableVersion(
            online_store_table_name="store_1",
            aggregation_result_name="result_2",
            version=2,
        ),
        OnlineStoreTableVersion(
            online_store_table_name="store_1",
            aggregation_result_name="result_3",
            version=3,
        ),
    ]


@pytest_asyncio.fixture
async def service_with_documents(online_store_table_version_service, online_store_table_versions):
    """
    Fixture to create documents in the service
    """
    for model in online_store_table_versions:
        await online_store_table_version_service.create_document(model)


@pytest.mark.usefixtures("service_with_documents")
@pytest.mark.asyncio
async def test_get_version__existing(online_store_table_version_service):
    """
    Test get_version for an existing aggregation result name
    """
    version = await online_store_table_version_service.get_version("result_1")
    assert version == 1


@pytest.mark.usefixtures("service_with_documents")
@pytest.mark.asyncio
async def test_get_version__non_existing(online_store_table_version_service):
    """
    Test get_version for a non existing aggregation result name
    """
    version = await online_store_table_version_service.get_version("result_non_existing")
    assert version is None


@pytest.mark.usefixtures("service_with_documents")
@pytest.mark.asyncio
async def test_get_versions(online_store_table_version_service):
    """
    Test get_versions for a list of aggregation result names
    """
    versions = await online_store_table_version_service.get_versions(["result_1", "result_3"])
    assert versions == {"result_1": 1, "result_3": 3}


@pytest.mark.usefixtures("service_with_documents")
@pytest.mark.asyncio
async def test_update_version(online_store_table_version_service):
    """
    Test update_version for an existing aggregation result name
    """
    await online_store_table_version_service.update_version("result_1", 2)
    version = await online_store_table_version_service.get_version("result_1")
    assert version == 2


@pytest.mark.usefixtures("service_with_documents")
@pytest.mark.asyncio
async def test_update_version__non_existing(online_store_table_version_service):
    """
    Test update_version for a non existing aggregation result name
    """
    with pytest.raises(DocumentNotFoundError) as exc_info:
        await online_store_table_version_service.update_version("result_non_existing", 2)
    assert str(exc_info.value) == "Aggregation result name not found"

"""
Test catalog service
"""

import pytest

from featurebyte.exception import DocumentNotFoundError


@pytest.mark.asyncio
async def test_catalog_get_and_list_raw_query_filter(app_container, catalog):
    """
    Test catalog service get query filter
    """
    catalog_service = app_container.catalog_service
    await catalog_service.soft_delete(document_id=catalog.id)

    with pytest.raises(DocumentNotFoundError):
        await catalog_service.get_document(document_id=catalog.id)

    with catalog_service.allow_use_raw_query_filter():
        # test get
        retrieved_catalog = await catalog_service.get_document(
            document_id=catalog.id, use_raw_query_filter=True
        )
        assert retrieved_catalog.id == catalog.id

        # test list
        output = await catalog_service.list_documents_as_dict(use_raw_query_filter=True)
        assert output["total"] == 1
        assert output["data"][0]["_id"] == catalog.id

        # test list iterator
        output = [
            catalog.id
            async for catalog in catalog_service.list_documents_iterator(
                query_filter={}, use_raw_query_filter=True
            )
        ]
        assert output == [catalog.id]

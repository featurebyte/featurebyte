"""
Tests functions/methods in routes/common directory
"""
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from featurebyte.routes.common.base import BaseController


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_filter, doc_represent, get_type, expected_msg",
    [
        (
            {"_id": "id_val"},
            {"id": "id_val"},
            "id",
            (
                'Col (id: "id_val") already exists. '
                'Get the existing object by `Col.get_by_id(id="conflict_id_val")`.'
            ),
        ),
        (
            {"_id": "id_val"},
            {"id": "id_val", "name": "name_val"},
            "id",
            (
                'Col (id: "id_val", name: "name_val") already exists. '
                'Get the existing object by `Col.get_by_id(id="conflict_id_val")`.'
            ),
        ),
        (
            {"_id": "id_val"},
            {"id": "id_val"},
            "name",
            (
                'Col (id: "id_val") already exists. '
                'Get the existing object by `Col.get(name="conflict_name_val")`.'
            ),
        ),
    ],
)
async def test_check_document_creation_conflict(
    query_filter, doc_represent, get_type, expected_msg
):
    """
    Test check_document_creation_conflict error message
    """

    class Controller(BaseController):
        collection_name = "col"

    persistent = AsyncMock()
    persistent.find_one.return_value = {"_id": "conflict_id_val", "name": "conflict_name_val"}
    with pytest.raises(HTTPException) as exc:
        await Controller.check_document_creation_conflict(
            persistent=persistent,
            query_filter=query_filter,
            doc_represent=doc_represent,
            get_type=get_type,
        )
    assert expected_msg in str(exc.value.detail)

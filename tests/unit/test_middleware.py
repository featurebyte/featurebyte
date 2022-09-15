"""
Test FastAPI app middleware
"""
import copy
from http import HTTPStatus

import pytest

from featurebyte.exception import DocumentConflictError, DocumentError
from featurebyte.middleware import ExecutionContext


@pytest.fixture(name="mock_exception_context")
def mock_insert_feature_list_registry_fixture():
    """
    Fixture for ExecutionContext
    """

    exception_handlers = copy.deepcopy(ExecutionContext.exception_handlers)
    ExecutionContext.exception_handlers.clear()

    yield ExecutionContext

    ExecutionContext.exception_handlers = exception_handlers


@pytest.mark.asyncio
async def test_register_exception_handler(mock_exception_context):
    """
    Test registering exception handler
    """

    mock_exception_context.register(DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT)
    mock_exception_context.register(
        DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY
    )

    assert len(mock_exception_context.exception_handlers) == 2
    assert list(mock_exception_context.exception_handlers.keys()) == [
        DocumentConflictError,
        DocumentError,
    ]


@pytest.mark.asyncio
async def test_register_exception_handler_register_non_exception(mock_exception_context):
    """
    Test registering exception handler
    """

    with pytest.raises(ValueError) as excinfo:
        mock_exception_context.register(HTTPStatus, handle_status_code=HTTPStatus.CONFLICT)

    assert (
        str(excinfo.value)
        == "registered key Type <enum 'HTTPStatus'> must be a subtype of Exception"
    )


@pytest.mark.asyncio
async def test_register_exception_handler_register_before_super_class(mock_exception_context):
    """
    Test registering exception handler
    """

    mock_exception_context.register(
        DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY
    )
    with pytest.raises(ValueError) as excinfo:
        mock_exception_context.register(
            DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT
        )

    assert (
        str(excinfo.value)
        == "<class 'featurebyte.exception.DocumentConflictError'> must be registered before its super class "
        "<class 'featurebyte.exception.DocumentError'>"
    )

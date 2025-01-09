"""
Test FastAPI app middleware
"""

import copy
from http import HTTPStatus

import pytest

from featurebyte.exception import DocumentConflictError, DocumentCreationError, DocumentError, FeatureByteException, \
    BaseConflictError
from featurebyte.middleware import ExecutionContext


@pytest.fixture(name="mock_exception_context")
def mock_exception_context_fixture():
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
    mock_exception_context.register(DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

    assert len(mock_exception_context.exception_handlers) == 2
    assert list(map(lambda x: x[0], mock_exception_context.exception_handlers)) == [DocumentConflictError, DocumentError]

@pytest.mark.asyncio
async def test_order_exception_handler(mock_exception_context):
    """
    Test registering exception handler

    FeaturebyteException -> BaseConflictError -> DocumentConflictError
    """

    mock_exception_context.register(FeatureByteException, handle_status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
    mock_exception_context.register(DocumentConflictError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)
    mock_exception_context.register(BaseConflictError, handle_status_code=HTTPStatus.CONFLICT)


    assert len(mock_exception_context.exception_handlers) == 3
    assert list(map(lambda x: x[0], mock_exception_context.exception_handlers)) == [DocumentConflictError, BaseConflictError, FeatureByteException]

@pytest.mark.asyncio
async def test_duplicated_exception_handler(mock_exception_context):
    """
    Test registering exception handler
    """
    mock_exception_context.register(DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT)
    with pytest.raises(ValueError) as excinfo:
        mock_exception_context.register(DocumentConflictError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

    assert (
        str(excinfo.value)
        == "Exception <class 'featurebyte.exception.DocumentConflictError'> has already registered"
    )


@pytest.mark.asyncio
async def test_register_exception_handler_register_non_exception(mock_exception_context):
    """
    Test registering exception handler
    """

    with pytest.raises(ValueError) as excinfo:
        mock_exception_context.register(HTTPStatus, handle_status_code=HTTPStatus.CONFLICT)

    assert (
        str(excinfo.value)
        == "<enum 'HTTPStatus'> must be a subtype of Exception"
    )

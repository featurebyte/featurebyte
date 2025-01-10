"""
Test FastAPI app middleware
"""

import copy
from http import HTTPStatus

import pytest

from featurebyte.exception import (
    BaseConflictError,
    DocumentConflictError,
    DocumentError,
    FeatureByteException,
)
from featurebyte.middleware import ExceptionMiddleware


@pytest.fixture(name="mock_exception_middleware_fixture")
def mock_exception_middleware_fixture():
    """
    Fixture for ExecutionContext
    """

    exception_handlers = copy.deepcopy(ExceptionMiddleware.exception_handlers)
    ExceptionMiddleware.exception_handlers.clear()

    yield ExceptionMiddleware

    ExceptionMiddleware.exception_handlers = exception_handlers


@pytest.mark.asyncio
async def test_register_exception_handler(mock_exception_middleware_fixture):
    """
    Test registering exception handler
    """

    mock_exception_middleware_fixture.register(
        DocumentConflictError, status_code=HTTPStatus.CONFLICT
    )
    mock_exception_middleware_fixture.register(
        DocumentError, status_code=HTTPStatus.UNPROCESSABLE_ENTITY
    )

    assert len(mock_exception_middleware_fixture.exception_handlers) == 2
    assert list(
        map(lambda x: x.exception_class, mock_exception_middleware_fixture.exception_handlers)
    ) == [
        DocumentConflictError,
        DocumentError,
    ]


@pytest.mark.asyncio
async def test_order_exception_handler(mock_exception_middleware_fixture):
    """
    Test registering exception handler

    FeaturebyteException -> BaseConflictError -> DocumentConflictError
    """

    mock_exception_middleware_fixture.register(
        FeatureByteException, status_code=HTTPStatus.INTERNAL_SERVER_ERROR
    )
    mock_exception_middleware_fixture.register(
        DocumentConflictError, status_code=HTTPStatus.UNPROCESSABLE_ENTITY
    )
    mock_exception_middleware_fixture.register(BaseConflictError, status_code=HTTPStatus.CONFLICT)

    assert len(mock_exception_middleware_fixture.exception_handlers) == 3
    assert list(
        map(lambda x: x.exception_class, mock_exception_middleware_fixture.exception_handlers)
    ) == [
        DocumentConflictError,
        BaseConflictError,
        FeatureByteException,
    ]


@pytest.mark.asyncio
async def test_duplicated_exception_handler(mock_exception_middleware_fixture):
    """
    Test registering exception handler
    """
    mock_exception_middleware_fixture.register(
        DocumentConflictError, status_code=HTTPStatus.CONFLICT
    )
    with pytest.raises(ValueError) as excinfo:
        mock_exception_middleware_fixture.register(
            DocumentConflictError, status_code=HTTPStatus.UNPROCESSABLE_ENTITY
        )

    assert (
        str(excinfo.value)
        == "Exception <class 'featurebyte.exception.DocumentConflictError'> has already registered"
    )


@pytest.mark.asyncio
async def test_register_exception_handler_register_non_exception(mock_exception_middleware_fixture):
    """
    Test registering exception handler
    """

    with pytest.raises(ValueError) as excinfo:
        mock_exception_middleware_fixture.register(
            HTTPStatus,
            status_code=HTTPStatus.CONFLICT,  # type: ignore
        )

    assert str(excinfo.value) == "<enum 'HTTPStatus'> must be a subtype of Exception"

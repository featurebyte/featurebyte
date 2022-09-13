"""
Test FastAPI app middleware
"""
from http import HTTPStatus

import pytest

from featurebyte.exception import DocumentConflictError, DocumentError
from featurebyte.middleware import ExecutionContext


@pytest.mark.asyncio
async def test_register_exception_handler():
    """
    Test registering exception handler
    """
    ExecutionContext.exception_handlers.clear()

    ExecutionContext.register(DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT)
    ExecutionContext.register(DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

    assert len(ExecutionContext.exception_handlers) == 2
    assert list(ExecutionContext.exception_handlers.keys()) == [
        DocumentConflictError,
        DocumentError,
    ]


@pytest.mark.asyncio
async def test_register_exception_handler_register_non_exception():
    """
    Test registering exception handler
    """
    ExecutionContext.exception_handlers.clear()

    with pytest.raises(ValueError) as excinfo:
        ExecutionContext.register(HTTPStatus, handle_status_code=HTTPStatus.CONFLICT)

    assert (
        str(excinfo.value)
        == "registered key Type <enum 'HTTPStatus'> must be a subtype of Exception"
    )


@pytest.mark.asyncio
async def test_register_exception_handler_register_before_super_class():
    """
    Test registering exception handler
    """
    ExecutionContext.exception_handlers.clear()

    ExecutionContext.register(DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)
    with pytest.raises(ValueError) as excinfo:
        ExecutionContext.register(DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT)

    assert str(excinfo.value) == "DocumentConflictError must be registered before its super class"

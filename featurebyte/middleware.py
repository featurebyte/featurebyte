"""
Handles API requests middleware
"""

import inspect
from http import HTTPStatus
from typing import Any, Awaitable, Callable, Dict, Optional, Type, Union, Tuple, List

from fastapi import FastAPI, Request, Response
from pydantic import ValidationError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from featurebyte.exception import (
    BaseConflictError,
    BaseFailedDependencyError,
    BaseUnprocessableEntityError,
    ColumnNotFoundError,
    DocumentNotFoundError,
    QueryNotSupportedError,
    TimeOutError,
)
from featurebyte.logging import get_logger

logger = get_logger(__name__)


class ExecutionContext:
    """
    ExecutionContext to handle exception and http status code globally
    """

    exception_handlers: List[Tuple[
        Type[BaseException],
        Union[int, Callable[[Request, BaseException], int]],
        Optional[Union[str, Callable[[Request, BaseException], str]]]]] = []

    @classmethod
    def register(
        cls,
        except_class: Any,
        handle_status_code: Union[int, Callable[[Request, BaseException], int]],
        handle_message: Optional[Union[str, Callable[[Request, BaseException], str]]] = None,
    ) -> None:
        """
        Register handlers for exception

        Parameters
        ----------
        except_class: Any
            exception class to be handled
        handle_status_code: Union[int, Callable[[Request], int]]
            http status code or a lambda function for customize status code
        handle_message: Union[str, Callable[[Request], str]]
            exception message or a lambda function for customize exception message

        Raises
        ----------
        ValueError
            when except_class has already been registered
            or when except_class is not a subtype of Exception
        """
        if not issubclass(except_class, BaseException):
            raise ValueError(f"{except_class} must be a subtype of Exception")

        inserted = False
        for i in range(len(cls.exception_handlers)):
            if except_class == cls.exception_handlers[i][0]:
                raise ValueError(f"Exception {except_class} has already registered")
            if issubclass(except_class, cls.exception_handlers[i][0]):
                inserted = True
                cls.exception_handlers = cls.exception_handlers[:i] + [(except_class, handle_status_code, handle_message)] + cls.exception_handlers[i:]
                break

        if not inserted:
            cls.exception_handlers.append((except_class, handle_status_code, handle_message))

    @classmethod
    def unregister(cls, except_class: Any) -> None:
        """
        Un-Register handlers for exception

        Parameters
        ----------
        except_class: Any
            exception class to be un-register
        """
        cls.exception_handlers.pop(except_class)

    def __init__(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]):
        self.request = request
        self.call_next = call_next

    async def execute(self) -> Response:
        """
        Actual executor of request/response. When catching exception, it will iterate through the exception handlers
        and return the http status code and message accordingly

        Raises
        -------
        Exception
            original un-handled exception

        Returns
        -------
        Response
            http response
        """
        try:
            return await self.call_next(self.request)
        except Exception as exc:
            for except_class, handle_status_code, handle_message in self.exception_handlers:
                if isinstance(exc, except_class):
                    if isinstance(handle_status_code, int):
                        status_code = handle_status_code
                    else:
                        status_code = handle_status_code(self.request, exc)

                    if handle_message is not None:
                        if isinstance(handle_message, str):
                            message = handle_message
                        else:
                            message = handle_message(self.request, exc)
                    else:
                        message = str(exc)

                    return JSONResponse(content={"detail": message}, status_code=status_code)

            # Default exception handling
            return JSONResponse(content={"detail": str(exc)}, status_code=HTTPStatus.INTERNAL_SERVER_ERROR)

    async def __aenter__(self) -> Any:
        """
        Signature method for async context manager

        Returns
        -------
            self
        """
        return self

    async def __aexit__(self, *exc: Dict[str, Any]) -> bool:
        """
        Signature method for async context manager

        Parameters
        ----------
        exc: Dict[str, Any]
            parameters

        Returns
        -------
            bool
        """
        return False


# UNPROCESSABLE_ENTITY errors
ExecutionContext.register(
    DocumentNotFoundError,
    handle_status_code=lambda req, exc: (
        HTTPStatus.UNPROCESSABLE_ENTITY if req.method == "POST" else HTTPStatus.NOT_FOUND
    ),
)

ExecutionContext.register(
    ColumnNotFoundError,
    handle_status_code=lambda req, exc: (
        HTTPStatus.UNPROCESSABLE_ENTITY if req.method == "POST" else HTTPStatus.NOT_FOUND
    ),
)

ExecutionContext.register(ValidationError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

ExecutionContext.register(
    BaseUnprocessableEntityError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY
)


# CONFLICT errors
ExecutionContext.register(BaseConflictError, handle_status_code=HTTPStatus.CONFLICT)


# NOT_IMPLEMENTED errors
ExecutionContext.register(
    QueryNotSupportedError,
    handle_status_code=HTTPStatus.NOT_IMPLEMENTED,
    handle_message="Query not supported.",
)


# FAILED_DEPENDENCY errors
ExecutionContext.register(
    BaseFailedDependencyError,
    handle_status_code=HTTPStatus.FAILED_DEPENDENCY,
)


# TimeoutError errors
ExecutionContext.register(
    TimeOutError,
    handle_status_code=HTTPStatus.REQUEST_TIMEOUT,
)


class ExceptionMiddleware(BaseHTTPMiddleware):
    """
    Middleware used by FastAPI to process each request
    """

    def __init__(self, app: FastAPI):
        super().__init__(app)

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """
        Exception middleware "main" call

        Parameters
        ----------
        request: Request
            Request object to be handled
        call_next: Callable[[Request], Awaitable[Response]]
            Function that will handle the request

        Returns
        -------
        Response
        """
        async with ExecutionContext(request, call_next) as executor:
            return await executor.execute()

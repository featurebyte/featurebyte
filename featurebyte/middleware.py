"""
Handles API requests middleware
"""
from typing import Any, Awaitable, Callable, Dict, Optional, Type, Union

import inspect
from http import HTTPStatus

from fastapi import Request, Response
from starlette.responses import JSONResponse

from featurebyte.exception import (
    DocumentConflictError,
    DocumentError,
    DocumentInconsistencyError,
    DocumentNotFoundError,
    DocumentUpdateError,
)


class ExecutionContext:
    """
    ExecutionContext to handle exception and http status code globally
    """

    exception_handlers: Dict[Type[Exception], Any] = {}

    @classmethod
    def register(
        cls,
        except_class: Any,
        handle_status_code: Union[int, Callable[[Request, Exception], int]],
        handle_message: Optional[Union[str, Callable[[Request, Exception], str]]] = None,
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
            when except_class is not a subclass of Exception
        """
        if not issubclass(except_class, Exception):
            raise ValueError(f"registered key Type {except_class} must be a subtype of Exception")

        if except_class in inspect.getmro(except_class)[1:-3]:
            raise ValueError(f"{except_class} must be registered before its super class")

        cls.exception_handlers[except_class] = (handle_status_code, handle_message)

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

    async def execute(self) -> Optional[Response]:
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
        except Exception as exc:  # pylint: disable=broad-except

            for except_class, (
                handle_status_code,
                handle_message,
            ) in self.exception_handlers.items():

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

            raise exc

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


ExecutionContext.register(DocumentConflictError, handle_status_code=HTTPStatus.CONFLICT)

ExecutionContext.register(
    DocumentNotFoundError,
    handle_status_code=lambda req, exc: HTTPStatus.UNPROCESSABLE_ENTITY
    if req.method == "POST"
    else HTTPStatus.NOT_FOUND,
)

ExecutionContext.register(DocumentError, handle_status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

ExecutionContext.register(
    NotImplementedError,
    handle_status_code=HTTPStatus.NOT_IMPLEMENTED,
    handle_message="Query not supported.",
)


async def request_handler(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    """Middleware used by FastAPI to process each request, featuring:

    - Handle Exception Globally

    Parameters
    ----------
    request: Request
        Request object to be handled
    call_next: Callable[[Request], Awaitable[Response]]
        Function that will handle the request

    Returns
    -------
    Response
        Response object to be returned to client
    """

    try:
        async with ExecutionContext(request, call_next) as executor:
            response: Response = await executor.execute()
    except Exception as exc:  # pylint: disable=broad-except
        response = JSONResponse(
            content={"detail": str(exc)}, status_code=HTTPStatus.INTERNAL_SERVER_ERROR
        )

    return response

"""
Handles API requests middleware
"""

from http import HTTPStatus
from typing import Awaitable, Callable, List, Optional, Type, Union

from fastapi import Request, Response
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


class ApiExceptionProcessor:
    """
    Exception processor class

    All registered exception handlers processed by api will be processed by this class
    """

    @staticmethod
    def default_response(_: BaseException, status_code: int, message: str) -> Response:
        """
        Default response for exception

        Parameters
        ----------
        _: BaseException
            Exception object
        status_code: int
            HTTP status code
        message: str
            Exception message

        Returns
        -------
        Response
        """
        return JSONResponse(content={"detail": message}, status_code=status_code)

    @staticmethod
    def default_message(_: Request, exc: BaseException) -> str:
        """
        Default exception message

        Parameters
        ----------
        _: Request
            Request object
        exc: BaseException
            Exception object

        Returns
        -------
        str
        """
        return str(exc)

    def __init__(
        self,
        exception_class: Type[BaseException],
        status_code: Union[int, Callable[[Request, BaseException], int]],
        message: Union[str, Callable[[Request, BaseException], str]],
        response: Callable[[BaseException, int, str], Response],
    ):
        self.exception_class = exception_class
        self.status_code = status_code
        self.response = response
        self.message = message

    def handle(self, request: Request, exc: BaseException) -> Response:
        """
        Handle exception returning a response

        Parameters
        ----------
        request: Request
            Request object
        exc: BaseException
            Exception object

        Returns
        -------
        Response
        """
        status_code = (
            self.status_code
            if isinstance(self.status_code, int)
            else self.status_code(request, exc)
        )
        message = self.message if isinstance(self.message, str) else self.message(request, exc)
        return self.response(exc, status_code, message)


class ExceptionMiddleware(BaseHTTPMiddleware):
    """
    Middleware used by FastAPI to handle and manage exceptions

    This middleware will handle exceptions and return a response to the client
    No other middleware should be mutating the response after this middleware
    """

    exception_handlers: List[ApiExceptionProcessor] = []

    @classmethod
    def register(
        cls,
        exception_class: Type[BaseException],
        status_code: Union[int, Callable[[Request, BaseException], int]],
        message: Optional[Union[str, Callable[[Request, BaseException], str]]] = None,
        response: Optional[Callable[[BaseException, int, str], Response]] = None,
    ) -> None:
        """
        Register handlers for exception

        Parameters
        ----------
        exception_class: Any
            exception class to be handled
        status_code: Union[int, Callable[[Request, BaseException], int]]
            http status code or a lambda function for customize status code
        message: Optional[Union[str, Callable[[Request, BaseException], str]]]
            exception message or a lambda function for customize exception message
            defaults to ApiExceptionProcessor.default_message
        response: Optional[Callable[[BaseException, int, str], Response]]
            function to generate response, defaults to ApiExceptionProcessor.default_response

        Raises
        ----------
        ValueError
            when except_class has already been registered
            or when except_class is not a subtype of Exception
        """
        if not issubclass(exception_class, BaseException):
            raise ValueError(f"{exception_class} must be a subtype of Exception")

        new_api_exception_processor = ApiExceptionProcessor(
            exception_class,
            status_code,
            message if message else ApiExceptionProcessor.default_message,
            response if response else ApiExceptionProcessor.default_response,
        )
        inserted = False
        for i in range(len(cls.exception_handlers)):
            if exception_class == cls.exception_handlers[i].exception_class:
                raise ValueError(f"Exception {exception_class} has already registered")
            if issubclass(exception_class, cls.exception_handlers[i].exception_class):
                inserted = True
                cls.exception_handlers.insert(i, new_api_exception_processor)
                break

        if not inserted:
            cls.exception_handlers.append(new_api_exception_processor)

    @classmethod
    def unregister(cls, except_class: Type[BaseException]) -> None:
        """
        Un-Register handlers for exception

        Parameters
        ----------
        except_class: Type[BaseException]
            exception class to be unregistered
        """
        for handler in cls.exception_handlers:
            if except_class == handler.exception_class:
                cls.exception_handlers.remove(handler)
                break

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
        try:
            return await call_next(request)
        except BaseException as exc:
            # Log exception
            logger.exception(str(exc))

            # Setting exception info in request state
            # This will be used by logging middleware to print execption info
            request.state.exc_info = exc
            for exception_handler in self.exception_handlers:
                # Found a matching exception
                if isinstance(exc, exception_handler.exception_class):
                    return exception_handler.handle(request, exc)

            # Default exception handling
            return JSONResponse(
                content={"detail": str(exc)}, status_code=HTTPStatus.INTERNAL_SERVER_ERROR
            )


# UNPROCESSABLE_ENTITY errors
ExceptionMiddleware.register(
    DocumentNotFoundError,
    status_code=lambda req, exc: (
        HTTPStatus.UNPROCESSABLE_ENTITY if req.method == "POST" else HTTPStatus.NOT_FOUND
    ),
)

ExceptionMiddleware.register(
    ColumnNotFoundError,
    status_code=lambda req, exc: (
        HTTPStatus.UNPROCESSABLE_ENTITY if req.method == "POST" else HTTPStatus.NOT_FOUND
    ),
)

ExceptionMiddleware.register(ValidationError, status_code=HTTPStatus.UNPROCESSABLE_ENTITY)

ExceptionMiddleware.register(
    BaseUnprocessableEntityError, status_code=HTTPStatus.UNPROCESSABLE_ENTITY
)


# CONFLICT errors
ExceptionMiddleware.register(BaseConflictError, status_code=HTTPStatus.CONFLICT)


# NOT_IMPLEMENTED errors
ExceptionMiddleware.register(
    QueryNotSupportedError,
    status_code=HTTPStatus.NOT_IMPLEMENTED,
    message="Query not supported.",
)


# FAILED_DEPENDENCY errors
ExceptionMiddleware.register(
    BaseFailedDependencyError,
    status_code=HTTPStatus.FAILED_DEPENDENCY,
)


# TimeoutError errors
ExceptionMiddleware.register(
    TimeOutError,
    status_code=HTTPStatus.REQUEST_TIMEOUT,
)

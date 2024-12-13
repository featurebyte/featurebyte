"""
Handles API requests middleware
"""

from http import HTTPStatus

from fastapi import Request
from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

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


class ExceptionMiddleware:
    """
    Middleware used by FastAPI to process each request
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await self.app(scope, receive, send)
        except TimeOutError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.REQUEST_TIMEOUT,
            )
            await response(scope, receive, send)
        except BaseFailedDependencyError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.FAILED_DEPENDENCY,
            )
            await response(scope, receive, send)
        except QueryNotSupportedError:
            response = JSONResponse(
                content={"detail": "Query not supported."},
                status_code=HTTPStatus.NOT_IMPLEMENTED,
            )
            await response(scope, receive, send)
        except BaseConflictError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.CONFLICT,
            )
            await response(scope, receive, send)
        except BaseUnprocessableEntityError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            await response(scope, receive, send)
        except ValidationError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
            await response(scope, receive, send)
        except ColumnNotFoundError as exc:
            request = Request(scope, receive)
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY
                if request.method == "POST"
                else HTTPStatus.NOT_FOUND,
            )
            await response(scope, receive, send)
        except DocumentNotFoundError as exc:
            response = JSONResponse(
                content={"detail": str(exc)},
                status_code=HTTPStatus.NOT_FOUND,
            )
            await response(scope, receive, send)

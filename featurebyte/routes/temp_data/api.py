"""
Temp data route
"""

from __future__ import annotations

from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from featurebyte.routes.base_router import BaseRouter


class TempDataRouter(BaseRouter):
    """
    Temp data router
    """

    def __init__(self) -> None:
        super().__init__(router=APIRouter(prefix="/temp_data"))
        self.router.add_api_route(
            "",
            self.get_data,
            methods=["GET"],
            status_code=HTTPStatus.OK,
        )

    @staticmethod
    async def get_data(request: Request, path: Path = Query()) -> StreamingResponse:
        """
        Retrieve temp data
        """
        controller = request.state.app_container.temp_data_controller
        result: StreamingResponse = await controller.get_data(
            path=path,
        )
        return result

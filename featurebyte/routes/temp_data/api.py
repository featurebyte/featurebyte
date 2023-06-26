"""
Temp data route
"""
from __future__ import annotations

from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/temp_data")


@router.get("", status_code=HTTPStatus.OK)
async def get_data(request: Request, path: Path = Query()) -> StreamingResponse:
    """
    Retrieve temp data
    """
    controller = request.state.app_container.temp_data_controller
    result: StreamingResponse = await controller.get_data(
        path=path,
    )
    return result

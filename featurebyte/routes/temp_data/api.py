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
    result: StreamingResponse = await request.state.controller.get_data(
        temp_storage=request.state.get_temp_storage(),
        path=path,
    )
    return result

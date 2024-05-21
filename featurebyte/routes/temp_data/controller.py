"""
Temp Data API route controller
"""

from __future__ import annotations

import mimetypes
from http import HTTPStatus
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from featurebyte.storage.base import Storage


class TempDataController:
    """
    TempDataController
    """

    def __init__(self, temp_storage: Storage):
        self.temp_storage = temp_storage

    async def get_data(
        self,
        path: Path,
    ) -> StreamingResponse:
        """
        Retrieve data in temp storage

        Parameters
        ----------
        path: Path
            Path of remote data object to retrieve

        Returns
        -------
        StreamingResponse
            StreamingResponse object

        Raises
        ------
        HTTPException
            Path is invalid
        """
        media_type = mimetypes.types_map.get(path.suffix, "application/octet-stream")
        try:
            bytestream = self.temp_storage.get_file_stream(remote_path=path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Invalid path") from exc
        return StreamingResponse(
            bytestream,
            media_type=media_type,
            headers={"content-disposition": f"filename={path.name}"},
        )

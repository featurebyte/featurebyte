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


class TempDataController:  # pylint: disable=too-few-public-methods
    """
    TempDataController
    """

    @classmethod
    async def get_data(
        cls,
        temp_storage: Storage,
        path: Path,
    ) -> StreamingResponse:
        """
        Retrieve data in temp storage

        Parameters
        ----------
        temp_storage: Storage
            Storage object
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
            bytestream = temp_storage.get_file_stream(remote_path=path)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Invalid path") from exc
        return StreamingResponse(bytestream, media_type=media_type)

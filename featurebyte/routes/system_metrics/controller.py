"""
System metrics API route controller
"""

from __future__ import annotations

from typing import Any, AsyncGenerator, List, Optional, Tuple

from featurebyte.models.system_metrics import SystemMetricsModel
from featurebyte.persistent.base import SortDir
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.system_metrics import SystemMetricsList
from featurebyte.service.system_metrics import SystemMetricsService


class SystemMetricsController(
    BaseDocumentController[SystemMetricsModel, SystemMetricsService, SystemMetricsList]
):
    """
    SystemMetricsList controller
    """

    paginated_document_class = SystemMetricsList

    async def download(
        self,
        sort_by: Optional[List[Tuple[str, SortDir]]] = None,
        **kwargs: Any,
    ) -> AsyncGenerator[bytes, None]:
        """
        Download metrics documents as an encoded JSON file.

        Parameters
        ----------
        sort_by: Optional[List[Tuple[str, SortDir]]]
            Keys and directions used to sort the returning documents
        kwargs: Any
            Additional keyword arguments

        Yields
        -------
        AsyncGenerator[bytes, None]
            An async generator yielding bytes of the JSON file
        """
        sort_by = sort_by or [("created_at", "desc")]
        first_line = True
        async for document in self.service.list_documents_iterator(
            query_filter={},
            sort_by=sort_by,
            **kwargs,
        ):
            if first_line:
                yield b"[\n"
                first_line = False
            else:
                yield b",\n"
            # Convert the document data to JSON bytes
            yield document.model_dump_json(by_alias=True).encode("utf-8")
        yield b"]\n"

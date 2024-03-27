"""
Table API route controller
"""

from __future__ import annotations

from featurebyte.models.proxy_table import ProxyTableModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.table import TableList
from featurebyte.service.table import TableService


class TableController(BaseDocumentController[ProxyTableModel, TableService, TableList]):
    """
    Table controller
    """

    paginated_document_class = TableList

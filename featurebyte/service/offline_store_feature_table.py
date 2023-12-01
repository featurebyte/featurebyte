"""
OfflineStoreFeatureTableService class
"""
from __future__ import annotations

from featurebyte.models.offline_store_feature_table import (
    OfflineStoreFeatureTableModel,
    OfflineStoreFeatureTableUpdate,
)
from featurebyte.service.base_document import BaseDocumentService


class OfflineStoreFeatureTableService(
    BaseDocumentService[
        OfflineStoreFeatureTableModel, OfflineStoreFeatureTableModel, OfflineStoreFeatureTableUpdate
    ]
):
    """
    OfflineStoreFeatureTableService class
    """

    document_class = OfflineStoreFeatureTableModel
    document_update_class = OfflineStoreFeatureTableUpdate

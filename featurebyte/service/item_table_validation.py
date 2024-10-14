"""
ItemTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.item_table import ItemTableModel
from featurebyte.schema.item_table import ItemTableCreate, ItemTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class ItemTableValidationService(
    BaseTableValidationService[ItemTableModel, ItemTableCreate, ItemTableServiceUpdate]
):
    """
    ItemTableValidationService class
    """

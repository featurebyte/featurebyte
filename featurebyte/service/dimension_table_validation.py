"""
DimensionTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.schema.dimension_table import DimensionTableCreate, DimensionTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class DimensionTableValidationService(
    BaseTableValidationService[
        DimensionTableModel, DimensionTableCreate, DimensionTableServiceUpdate
    ]
):
    """
    DimensionTableValidationService class
    """

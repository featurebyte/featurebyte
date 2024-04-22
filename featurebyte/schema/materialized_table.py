"""
Schemas common to materialized table
"""

from typing import Any, Dict, Tuple

from pydantic import root_validator

from featurebyte.models.base import FeatureByteBaseDocumentModel


class BaseMaterializedTableListRecord(FeatureByteBaseDocumentModel):
    """
    This model determines the schema when listing tables.
    """

    shape: Tuple[int, int]

    @root_validator(pre=True)
    @classmethod
    def _extract_common_materialized_table_fields(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["shape"] = values["num_rows"], len(values["columns_info"])
        return values

"""
Schemas common to materialized table
"""

from typing import Any, Tuple

from pydantic import BaseModel, model_validator

from featurebyte.models.base import FeatureByteBaseDocumentModel


class BaseMaterializedTableListRecord(FeatureByteBaseDocumentModel):
    """
    This model determines the schema when listing tables.
    """

    shape: Tuple[int, int]

    @model_validator(mode="before")
    @classmethod
    def _extract_common_materialized_table_fields(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        values["shape"] = values["num_rows"], len(values["columns_info"])
        return values

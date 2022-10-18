"""
This module contains ItemData related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, List, Optional

from pydantic import StrictStr, validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature_store import DataModel


class ItemDataModel(DataModel, FeatureByteBaseDocumentModel):
    """
    Model for ItemData entity

    id: PydanticObjectId
        EventData id of the object
    name : str
        Name of the EventData
    tabular_source : TabularSource
        Data warehouse connection information & table name tuple
    columns_info: List[ColumnInfo]
        List of event data columns
    status: DataStatus
        Status of the ItemData
    event_id_column: str
        Event ID column name
    item_id_column: str
        Item ID column name
    created_at : Optional[datetime]
        Datetime when the EventData was first saved or published
    updated_at: Optional[datetime]
        Datetime when the EventData object was last updated
    """

    event_id_column: StrictStr
    item_id_column: StrictStr

    @validator("event_id_column", "item_id_column")
    @classmethod
    def _check_column_exists(cls, value: Optional[str], values: dict[str, Any]) -> Optional[str]:
        columns = {dict(col)["name"] for col in values["columns_info"]}
        if value is not None and value not in columns:
            raise ValueError(f'Column "{value}" not found in the table!')
        return value

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "item_data"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("tabular_source",),
                conflict_fields_signature={"tabular_source": ["tabular_source"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

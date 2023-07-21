"""
Catalog API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.catalog import CatalogModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class CatalogCreate(FeatureByteBaseModel):
    """
    Catalog creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    default_feature_store_ids: List[PydanticObjectId]


class CatalogList(PaginationMixin):
    """
    Paginated list of Catalog
    """

    data: List[CatalogModel]


class CatalogUpdate(FeatureByteBaseModel):
    """
    Catalog update schema
    """

    name: StrictStr


class CatalogServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Catalog service update schema
    """

    name: Optional[StrictStr]

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique contraints checking
        """

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

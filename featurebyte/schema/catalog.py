"""
Catalog API payload schema
"""

from __future__ import annotations

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
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

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    default_feature_store_ids: list[PydanticObjectId]
    online_store_id: PydanticObjectId | None = Field(default=None)


class CatalogList(PaginationMixin):
    """
    Paginated list of Catalog
    """

    data: list[CatalogModel]


class CatalogUpdate(FeatureByteBaseModel):
    """
    Catalog update schema
    """

    name: NameStr | None = Field(default=None)


class CatalogOnlineStoreUpdate(BaseDocumentServiceUpdateSchema):
    """
    Catalog update online store schema
    """

    online_store_id: PydanticObjectId | None = Field(default=None)


class CatalogServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Catalog service update schema
    """

    name: NameStr | None = Field(default=None)

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique constraints checking
        """

        unique_constraints: list[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

"""
Schema for Development Dataset.
"""

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.development_dataset import DevelopmentTable
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)


class DevelopmentDatasetBase(FeatureByteBaseModel):
    """
    DevelopmentDataset base schema
    """

    name: NameStr
    description: Optional[StrictStr] = Field(default=None)
    sample_from_timestamp: datetime
    sample_to_timestamp: datetime
    development_tables: List[DevelopmentTable]


class DevelopmentDatasetCreate(DevelopmentDatasetBase):
    """
    DevelopmentDataset creation base schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")


class DevelopmentDatasetRead(DevelopmentDatasetBase):
    """
    DevelopmentDataset read payload schema
    """

    id: PydanticObjectId = Field(alias="_id")


class DevelopmentDatasetList(PaginationMixin):
    """
    Paginated list of DevelopmentDataset
    """

    data: List[DevelopmentDatasetRead]


class DevelopmentDatasetUpdate(FeatureByteBaseModel):
    """
    DevelopmentDataset update payload schema
    """

    name: Optional[NameStr] = Field(default=None)


class DevelopmentDatasetAddTables(FeatureByteBaseModel):
    """
    DevelopmentDataset add tables payload schema
    """

    development_tables: List[DevelopmentTable] = Field(default_factory=list)


class DevelopmentDatasetServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    DevelopmentDatasetService update payload schema
    """

    name: Optional[NameStr] = Field(default=None)
    development_tables: Optional[List[DevelopmentTable]] = Field(default=None)


class DevelopmentTableInfo(FeatureByteBaseModel):
    """
    DevelopmentTableInfo schema
    """

    table_name: str
    feature_store_name: str
    table_details: TableDetails


class DevelopmentDatasetInfo(BaseInfo):
    """
    DevelopmentDataset info schema
    """

    sample_from_timestamp: datetime
    sample_to_timestamp: datetime
    development_tables: List[DevelopmentTableInfo]

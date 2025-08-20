"""
Schema for Development Dataset.
"""

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.development_dataset import (
    DevelopmentDatasetSourceType,
    DevelopmentDatasetStatus,
    DevelopmentTable,
)
from featurebyte.query_graph.model.common_table import TabularSource
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
    status: DevelopmentDatasetStatus = Field(default=DevelopmentDatasetStatus.ACTIVE)
    source_type: DevelopmentDatasetSourceType = Field(
        default=DevelopmentDatasetSourceType.SOURCE_TABLES
    )
    development_plan_id: Optional[PydanticObjectId] = None
    observation_table_id: Optional[PydanticObjectId] = None


class DevelopmentTableCreate(FeatureByteBaseModel):
    """
    Development source table for a table
    """

    table_id: PydanticObjectId
    location: TabularSource
    sampled_table_id: Optional[PydanticObjectId] = None


class DevelopmentDatasetCreate(DevelopmentDatasetBase):
    """
    DevelopmentDataset creation base schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    development_tables: List[DevelopmentTableCreate]


class DevelopmentDatasetRead(DevelopmentDatasetBase):
    """
    DevelopmentDataset read payload schema
    """

    id: PydanticObjectId = Field(alias="_id")
    development_tables: List[DevelopmentTable]


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
    status: Optional[DevelopmentDatasetStatus] = Field(default=None)


class DevelopmentTableInfo(FeatureByteBaseModel):
    """
    DevelopmentTableInfo schema
    """

    table_name: str
    feature_store_name: str
    table_details: TableDetails
    deleted: bool = Field(default=False)
    sampled_table_id: Optional[PydanticObjectId] = None


class DevelopmentDatasetInfo(BaseInfo):
    """
    DevelopmentDataset info schema
    """

    sample_from_timestamp: datetime
    sample_to_timestamp: datetime
    development_tables: List[DevelopmentTableInfo]
    status: DevelopmentDatasetStatus = Field(default=DevelopmentDatasetStatus.ACTIVE)
    source_type: DevelopmentDatasetSourceType = Field(
        default=DevelopmentDatasetSourceType.SOURCE_TABLES
    )
    development_plan_id: Optional[PydanticObjectId] = None
    observation_table_id: Optional[PydanticObjectId] = None

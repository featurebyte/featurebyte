"""
Base info related schema
"""
# pylint: disable=too-few-public-methods
from typing import List, Optional

from datetime import datetime

from featurebyte.models.base import FeatureByteBaseModel, UniqueValuesConstraint


class BaseBriefInfo(FeatureByteBaseModel):
    """
    Base BriefInfo schema
    """

    name: str


class BaseInfo(BaseBriefInfo):
    """
    Base Info schema
    """

    created_at: datetime
    updated_at: Optional[datetime]


class BaseDocumentServiceUpdateSchema(FeatureByteBaseModel):
    """
    Base schema used for document service update
    """

    class Settings:
        """
        Unique constraints checking during update
        """

        unique_constraints: List[UniqueValuesConstraint] = []

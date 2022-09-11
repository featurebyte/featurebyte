"""
Base info related schema
"""
from typing import Optional

from datetime import datetime

from featurebyte.models.base import FeatureByteBaseModel


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

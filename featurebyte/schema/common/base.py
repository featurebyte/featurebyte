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

    creation_date: datetime
    update_date: Optional[datetime]

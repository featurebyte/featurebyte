"""
Progress payload schema
"""
from typing import Optional

from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel


class ProgressModel(FeatureByteBaseModel):
    """
    Progress schema
    """

    percent: int = Field(ge=0, le=100)
    message: Optional[StrictStr] = Field(default=None)

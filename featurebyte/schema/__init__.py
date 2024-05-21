"""
Payload and response schema for API routes
"""

from pydantic import BaseModel


class APIServiceStatus(BaseModel):
    """
    API Service Status
    """

    sdk_version: str

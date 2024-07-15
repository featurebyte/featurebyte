"""
This module contains Online Store related models
"""

from typing import ClassVar, List, Literal, Optional, Union

import pymongo
from pydantic import Field, StrictStr
from typing_extensions import Annotated

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import OnlineStoreType, RedisType
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.credential import BaseDatabaseCredential, UsernamePasswordCredential


class BaseOnlineStoreDetails(FeatureByteBaseModel):
    """
    Base class for online store details
    """

    # Online store type selector
    type: OnlineStoreType
    credential: Optional[BaseDatabaseCredential] = Field(default=None)

    def hide_details_credentials(self) -> None:
        """
        Hide credentials values
        """
        if self.credential:
            self.credential.hide_values()


class RedisOnlineStoreDetails(BaseOnlineStoreDetails):
    """
    Configuration details for Redis online store.

    Examples
    --------
    >>> details = fb.RedisOnlineStoreDetails(
    ...     redis_type="redis",
    ...     connection_string="localhost:6379",
    ...     key_ttl_seconds=3600,
    ...     credential=fb.UsernamePasswordCredential(
    ...         username="username",
    ...         password="password",
    ...     ),
    ... )
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.RedisOnlineStoreDetails"
    )

    # Online store type selector
    type: Literal[OnlineStoreType.REDIS] = OnlineStoreType.REDIS

    redis_type: RedisType = Field(
        default=RedisType.REDIS, description="Redis type: redis or redis_cluster."
    )
    #  format: host:port,parameter1,parameter2 eg. redis:6379,db=0
    connection_string: StrictStr = Field(
        default="localhost:6379",
        description="Connection string with format 'host:port,parameter1,parameter2' eg. redis:6379,db=0",
    )

    key_ttl_seconds: Optional[int] = Field(
        default=None, description="Redis key bin ttl (in seconds) for expiring entities."
    )

    credential: Optional[UsernamePasswordCredential] = Field(
        default=None, description="Redis user and password."
    )


class MySQLOnlineStoreDetails(BaseOnlineStoreDetails):
    """
    Configuration details for MySQL online store.

    Examples
    --------
    >>> details = fb.MySQLOnlineStoreDetails(
    ...     host="localhost",
    ...     database="database",
    ...     port=3306,
    ...     credential=fb.UsernamePasswordCredential(username="user", password="password"),
    ... )
    """

    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.MySQLOnlineStoreDetails"
    )

    # Online store type selector
    type: Literal[OnlineStoreType.MYSQL] = OnlineStoreType.MYSQL

    host: StrictStr = Field(default="localhost", description="MySQL connection host.")

    database: StrictStr = Field(description="MySQL connection database.")

    port: int = Field(default=3306, description="MySQL connection port.")

    credential: Optional[UsernamePasswordCredential] = Field(
        default=None, description="MySQL user and password."
    )


OnlineStoreDetails = Annotated[
    Union[RedisOnlineStoreDetails, MySQLOnlineStoreDetails],
    Field(discriminator="type"),
]


class OnlineStoreModel(FeatureByteBaseDocumentModel):
    """
    Model for Online Store
    """

    details: OnlineStoreDetails

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "online_store"
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
                fields=("details",),
                conflict_fields_signature={"details": ["details"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("details.type"),
            pymongo.operations.IndexModel("details"),
            [
                ("name", pymongo.TEXT),
                ("details.type", pymongo.TEXT),
            ],
        ]

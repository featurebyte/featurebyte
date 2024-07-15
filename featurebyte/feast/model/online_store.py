"""
Models to construct feast online store config from featurebyte BaseOnlineStoreDetails
"""

from __future__ import annotations

from typing import Union, cast
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from pydantic import Field, parse_obj_as

from featurebyte.feast.online_store.mysql import FBMySQLOnlineStoreConfig
from featurebyte.models.online_store import (
    BaseOnlineStoreDetails,
    MySQLOnlineStoreDetails,
    RedisOnlineStoreDetails,
)


class BaseOnlineStoreDetailsForFeast:
    """
    Base class for online store details for feast
    """

    @abstractmethod
    def to_feast_online_store_config(self) -> FeastConfigBaseModel:
        """
        Returns a corresponding online store configuration instance in feast
        """


class FeastRedisOnlineStoreDetails(BaseOnlineStoreDetailsForFeast, RedisOnlineStoreDetails):
    """
    Redis online store details
    """

    def to_feast_online_store_config(self) -> FeastConfigBaseModel:
        connection_string = self.connection_string
        if self.credential is not None:
            if self.credential.username:
                connection_string += f",username={self.credential.username}"
            if self.credential.password:
                connection_string += f",password={self.credential.password}"
        return RedisOnlineStoreConfig(connection_string=connection_string)


class FeastMySQLOnlineStoreDetails(BaseOnlineStoreDetailsForFeast, MySQLOnlineStoreDetails):
    """
    MySQL online store details
    """

    def to_feast_online_store_config(self) -> FeastConfigBaseModel:
        if self.credential is not None:
            user, password = self.credential.username, self.credential.password
        else:
            user, password = None, None
        config = FBMySQLOnlineStoreConfig(
            host=self.host,
            user=user,
            password=password,
            database=self.database,
            port=self.port,
        )
        return cast(FeastConfigBaseModel, config)


FeastOnlineStoreDetails = Annotated[
    Union[FeastRedisOnlineStoreDetails, FeastMySQLOnlineStoreDetails],
    Field(discriminator="type"),
]


def get_feast_online_store_details(
    online_store_details: BaseOnlineStoreDetails,
) -> FeastOnlineStoreDetails:
    """
    Create a FeastOnlineStoreDetails object given an instance of BaseOnlineStoreDetails

    Parameters
    ----------
    online_store_details: BaseOnlineStoreDetails
        Instance of BaseOnlineStoreDetails

    Returns
    -------
    FeastOnlineStoreDetails
    """
    return parse_obj_as(FeastOnlineStoreDetails, online_store_details.dict())  # type: ignore

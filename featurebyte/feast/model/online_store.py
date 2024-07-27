"""
Models to construct feast online store config from featurebyte BaseOnlineStoreDetails
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Union, cast

from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from pydantic import Field
from typing_extensions import Annotated

from featurebyte.common.model_util import construct_serialize_function
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
            username, password = self.credential.username, self.credential.password
            if username:
                connection_string += f",username={username}"
            if password:
                connection_string += f",password={password}"
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


ONLINE_STORE_DETAILS_TYPES = [FeastRedisOnlineStoreDetails, FeastMySQLOnlineStoreDetails]

if TYPE_CHECKING:
    # use FeastOnlineStoreDetails during type checking
    FeastOnlineStoreDetails = BaseOnlineStoreDetailsForFeast
else:
    # during runtime, use Annotated type for pydantic model deserialization
    FeastOnlineStoreDetails = Annotated[
        Union[tuple(ONLINE_STORE_DETAILS_TYPES)], Field(discriminator="type")
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
    params = online_store_details.model_dump(by_alias=True)
    construct_online_store_details = construct_serialize_function(
        all_types=ONLINE_STORE_DETAILS_TYPES,
        annotated_type=FeastOnlineStoreDetails,
        discriminator_key="type",
    )
    return cast(FeastOnlineStoreDetails, construct_online_store_details(**params))

"""
Models to construct feast online store config from featurebyte BaseOnlineStoreDetails
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union, cast
from typing_extensions import Annotated

from abc import abstractmethod  # pylint: disable=wrong-import-order

from feast.infra.online_stores.redis import RedisOnlineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from pydantic import Field, TypeAdapter

from featurebyte.common.model_util import get_type_to_class_map
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
            # pylint: disable=no-member
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
            # pylint: disable=no-member
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

# construct online store details class map for deserialization
ONLINE_STORE_DETAILS_CLASS_MAP = get_type_to_class_map(ONLINE_STORE_DETAILS_TYPES)


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
    params = online_store_details.dict(by_alias=True)
    online_store_detail_class = ONLINE_STORE_DETAILS_CLASS_MAP.get(params.get("type"))
    if online_store_detail_class is None:
        # use pydantic builtin version to throw validation error (slow due to pydantic V2 performance issue)
        return TypeAdapter(FeastOnlineStoreDetails).validate_python(params)

    # use internal method to avoid current pydantic V2 performance issue due to _core_utils.py:walk
    # https://github.com/pydantic/pydantic/issues/6768
    return cast(FeastOnlineStoreDetails, online_store_detail_class(**params))

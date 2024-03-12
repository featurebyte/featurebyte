"""
Test for feast online store models
"""
from feast.infra.online_stores.redis import RedisOnlineStoreConfig

from featurebyte import MySQLOnlineStoreDetails, RedisOnlineStoreDetails
from featurebyte.feast.model.online_store import get_feast_online_store_details
from featurebyte.feast.online_store.mysql import FBMySQLOnlineStoreConfig


def test_redis_details():
    """
    Test converting redis online store details to feast config
    """
    online_store_details = RedisOnlineStoreDetails(
        redis_type="redis",
        connection_string="localhost:6379",
        credential={"username": "user", "password": "my_pw"},
    )
    feast_config = get_feast_online_store_details(
        online_store_details
    ).to_feast_online_store_config()
    assert feast_config == RedisOnlineStoreConfig(
        type="redis",
        redis_type="redis",
        connection_string="localhost:6379,username=user,password=my_pw",
        key_ttl_seconds=None,
    )


def test_mysql_details():
    """
    Test converting mysql online store details to feast config
    """
    online_store_details = MySQLOnlineStoreDetails(
        host="1.2.3.4",
        database="my_db",
        port=3306,
        credential={"username": "user", "password": "my_pw"},
    )
    feast_config = get_feast_online_store_details(
        online_store_details
    ).to_feast_online_store_config()
    assert feast_config == FBMySQLOnlineStoreConfig(
        host="1.2.3.4",
        user="user",
        password="my_pw",
        database="my_db",
        port=3306,
    )

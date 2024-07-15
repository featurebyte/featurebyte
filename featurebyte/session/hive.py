"""
Customized Hive Connection class
"""

import logging
from ssl import CERT_NONE, create_default_context
from typing import Any, Mapping, Optional

from pyhive import hive
from pyhive.exc import OperationalError
from pyhive.hive import Connection
from pyhive.hive import Cursor as BaseCursor
from pyhive.hive import _logger as hive_logger
from thrift.transport.THttpClient import THttpClient
from thrift.transport.TTransport import TTransportBase
from typeguard import typechecked

from featurebyte.enum import StrEnum
from featurebyte.logging import get_logger

logger = get_logger(__name__)

# keep hive logger quiet
hive_logger.setLevel(logging.ERROR)


class AuthType(StrEnum):
    """
    Authentication Type
    """

    NONE = "NONE"
    BASIC = "BASIC"
    NOSASL = "NOSASL"
    KERBEROS = "KERBEROS"
    TOKEN = "TOKEN"


class Cursor(BaseCursor):
    """
    Customized Hive Cursor class to support additional functionality
    """

    def close(self) -> None:
        """
        Close the cursor
        """
        try:
            super().close()
        except Exception:
            logger.error("Failed to close cursor", exc_info=True)


class HiveConnection(Connection):
    """
    Customized Hive Connection class to support additional functionality
    """

    @typechecked
    def __init__(
        self: Any,
        host: Optional[str] = "127.0.0.1",
        port: Optional[int] = 10000,
        http_path: Optional[str] = "cliservice",
        scheme: Optional[str] = None,
        catalog: str = "spark_catalog",
        database: str = "default",
        auth: Optional[AuthType] = None,
        configuration: Optional[Mapping[str, Any]] = None,
        kerberos_service_name: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        access_token: Optional[str] = None,
        check_hostname: bool = False,
        ssl_cert: Optional[str] = None,
        thrift_transport: Optional[TTransportBase] = None,
    ) -> None:
        if scheme in ("https", "http") and not thrift_transport:
            port = port or 1000
            ssl_context = None
            if scheme == "https":
                ssl_context = create_default_context()
                ssl_context.check_hostname = check_hostname
                ssl_cert = ssl_cert or "none"
                ssl_context.verify_mode = hive.ssl_cert_parameter_map.get(ssl_cert, CERT_NONE)
            uri_or_host = f"{scheme}://{host}:{port}/{http_path}"
            logger.debug("Connecting to Hive server", extra={"uri_or_host": uri_or_host})
            thrift_transport = THttpClient(
                uri_or_host=uri_or_host,
                ssl_context=ssl_context,
            )

            if auth in (AuthType.BASIC, AuthType.NOSASL, AuthType.NONE):
                # Always needs the Authorization header
                self._set_authorization_header(thrift_transport, username, password)
            elif auth == AuthType.KERBEROS and kerberos_service_name:
                self._set_kerberos_header(thrift_transport, kerberos_service_name, host)
            elif auth == AuthType.TOKEN:
                thrift_transport.setCustomHeaders({"Authorization": f"Bearer {access_token}"})
            else:
                raise ValueError(f"Invalid value for auth: {auth}")
            host, port, auth, kerberos_service_name, password = (None, None, None, None, None)

        params = {
            "host": host,
            "port": port,
            "scheme": scheme,
            "username": username,
            "database": f"{catalog}`.`{database}",
            "auth": auth,
            "configuration": configuration,
            "kerberos_service_name": kerberos_service_name,
            "password": password,
            "check_hostname": check_hostname,
            "ssl_cert": ssl_cert,
            "thrift_transport": thrift_transport,
        }
        try:
            super().__init__(**params)
        except OperationalError:
            # retry using default database to create schema
            params["database"] = f"{catalog}`.`default"
            super().__init__(**params)
            cursor = self.cursor()
            cursor.execute(f"CREATE SCHEMA `{catalog}`.`{database}`")
            cursor.execute(f"USE `{catalog}`.`{database}`")
            cursor.close()

    def close(self) -> None:
        """
        Close the connection
        """
        try:
            super().close()
        except Exception:
            logger.error("Failed to close connection", exc_info=True)

    def cursor(self, *args: Any, **kwargs: Any) -> Cursor:
        """
        Create a cursor

        Parameters
        ----------
        args: Any
            Arguments
        kwargs: Any
            Keyword arguments

        Returns
        -------
        Cursor
        """
        return Cursor(self, *args, **kwargs)

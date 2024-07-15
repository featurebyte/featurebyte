"""
Customized smart_open WebHDFS client to support Kerberos authentication
"""

from http import HTTPStatus
from typing import Any, Dict, List

import requests
import requests_kerberos
from smart_open.webhdfs import (
    MIN_PART_SIZE,
    SCHEME,
    BufferedInputBase,
    BufferedOutputBase,
    WebHdfsException,
    _convert_to_http_uri,
    constants,
)

TIMEOUT = 600


def webhdfs_open(
    http_uri: str,
    mode: str,
    min_part_size: int = MIN_PART_SIZE,
    kerberos: bool = False,
    ssl: bool = False,
) -> Any:
    """
    Open file from webhdfs.

    Parameters
    ----------
    http_uri: str
        webhdfs url converted to http REST url
    mode: str
        File mode
    min_part_size: int
        For writing only.
    kerberos: bool
        Whether to use Kerberos authentication
    ssl: bool
        Whether to use SSL

    Returns
    -------
    Any
        File object

    Raises
    ------
    NotImplementedError
        If mode is not supported
    """
    if http_uri.startswith(SCHEME):
        http_uri = _convert_to_http_uri(http_uri)
    if ssl:
        http_uri = http_uri.replace("http://", "https://")

    if mode == constants.READ_BINARY:
        fobj = BufferedInput(http_uri, kerberos=kerberos)
    elif mode == constants.WRITE_BINARY:
        fobj = BufferedOutput(http_uri, min_part_size=min_part_size, kerberos=kerberos)
    else:
        raise NotImplementedError(f"webhdfs support for mode {mode} not implemented")
    return fobj


def webhdfs_delete(http_uri: str, kerberos: bool = False, ssl: bool = False) -> None:
    """
    Delete file from webhdfs

    Parameters
    ----------
    http_uri: str
        webhdfs url converted to http REST url
    kerberos: bool
        Whether to use Kerberos authentication
    ssl: bool
        Whether to use SSL

    Raises
    ------
    WebHdfsException
        If the file could not be deleted
    """
    if http_uri.startswith(SCHEME):
        http_uri = _convert_to_http_uri(http_uri)
    if ssl:
        http_uri = http_uri.replace("http://", "https://")

    kwargs = _get_request_params(kerberos=kerberos)
    payload = {"op": "DELETE"}
    response = requests.delete(http_uri, params=payload, timeout=TIMEOUT, **kwargs)
    if response.status_code != HTTPStatus.OK:
        raise WebHdfsException(msg=response.text, status_code=response.status_code)


def _get_request_params(kerberos: bool = False) -> Dict[str, Any]:
    """
    Get request parameters for webhdfs

    Parameters
    ----------
    kerberos: bool
        Whether to use Kerberos authentication

    Returns
    -------
    Dict[str, Any]
        Request parameters
    """
    kwargs = {"verify": False}  # ignore SSL certificate verification
    if kerberos:
        kwargs["auth"] = requests_kerberos.HTTPKerberosAuth()
    return kwargs


class BufferedInput(BufferedInputBase):
    """
    BufferedInput class for webhdfs with Kerberos authentication
    """

    def __init__(self, uri: str, kerberos: bool = False) -> None:
        """
        Initialize BufferedInput

        Parameters
        ----------
        uri: str
            webhdfs url converted to http REST url
        kerberos: bool
            Whether to use Kerberos authentication

        Raises
        ------
        WebHdfsException
            If the file could not be opened
        """
        self._uri = uri
        self.name = uri.split("/")[-1]

        payload = {"op": "OPEN", "offset": "0"}
        kwargs = _get_request_params(kerberos=kerberos)
        self._response = requests.get(
            self._uri, params=payload, stream=True, timeout=TIMEOUT, **kwargs
        )
        if self._response.status_code != HTTPStatus.OK:
            raise WebHdfsException(msg=self._response.text, status_code=self._response.status_code)
        self._buf = b""


class BufferedOutput(BufferedOutputBase):
    """
    BufferedOutput class for webhdfs with Kerberos authentication
    """

    def __init__(
        self, uri: str, min_part_size: int = MIN_PART_SIZE, kerberos: bool = False
    ) -> None:
        """
        Initialize BufferedOutput

        Parameters
        ----------
        uri: str
            webhdfs url converted to http REST url
        min_part_size: int
            For writing only.
        kerberos: bool
            Whether to use Kerberos authentication

        Raises
        ------
        WebHdfsException
            If the file could not be opened
        """
        self._uri = uri
        self._closed = False
        self._kerberos = kerberos
        self.name = uri.split("/")[-1]
        self.min_part_size = min_part_size
        self.lines: List[bytes] = []
        self.parts = 0
        self.chunk_bytes = 0
        self.total_size = 0
        self.raw = None

        # creating empty file first
        payload = {"op": "CREATE", "overwrite": "True"}
        kwargs = _get_request_params(kerberos=kerberos)
        init_response = requests.put(
            self._uri, params=payload, allow_redirects=False, timeout=TIMEOUT, **kwargs
        )
        if not init_response.status_code == HTTPStatus.TEMPORARY_REDIRECT:
            raise WebHdfsException(msg=init_response.text, status_code=init_response.status_code)
        uri = init_response.headers["location"]

        response = requests.put(
            uri,
            data="",
            headers={"content-type": "application/octet-stream"},
            timeout=TIMEOUT,
            **kwargs,
        )
        if not response.status_code == HTTPStatus.CREATED:
            raise WebHdfsException(msg=response.text, status_code=response.status_code)

    def _upload(self, data: bytes) -> None:
        payload = {"op": "APPEND"}
        kwargs = _get_request_params(kerberos=self._kerberos)
        init_response = requests.post(
            self._uri, params=payload, allow_redirects=False, timeout=TIMEOUT, **kwargs
        )
        if not init_response.status_code == HTTPStatus.TEMPORARY_REDIRECT:
            raise WebHdfsException(msg=init_response.text, status_code=init_response.status_code)
        uri = init_response.headers["location"]

        response = requests.post(
            uri,
            data=data,
            headers={"content-type": "application/octet-stream"},
            timeout=TIMEOUT,
            **kwargs,
        )
        if not response.status_code == HTTPStatus.OK:
            raise WebHdfsException(msg=response.text, status_code=response.status_code)

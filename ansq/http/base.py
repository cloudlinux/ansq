import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from ansq.typedefs import HTTPResponse
from .http_exceptions import HTTP_EXCEPTIONS, NSQHTTPException
from .unix_client import UnixHTTPConnection
from ..utils import convert_to_bytes, is_unix_socket

if TYPE_CHECKING:
    from asyncio.events import AbstractEventLoop

_T = TypeVar("_T", bound="NSQHTTPConnection")

HTTP_TIMEOUT = 10


class NSQHTTPConnection:
    """XXX"""

    def __init__(
        self,
        addr: str = "127.0.0.1:4151",
        *,
        loop: Optional["AbstractEventLoop"] = None,
    ) -> None:
        self._loop = loop or asyncio.get_event_loop()
        self._addr = addr
        self._is_unix_socket = is_unix_socket(self._addr)

    async def close(self) -> None:
        pass

    async def perform_request(
        self, method: str, url: str, params: Any, body: Any
    ) -> HTTPResponse:
        encoded_params = ""
        if params:
            encoded_params = "?" + urllib.parse.urlencode(params)

        return await self._loop.run_in_executor(
            None,
            _do_request,
            self._addr,
            method,
            urllib.parse.urljoin(url, encoded_params),
            convert_to_bytes(body) if body else body
        )

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name}: {self._addr}>"


def _do_request(addr: str, method: str, url: str, body: Any) -> HTTPResponse:
    if is_unix_socket(addr):
        with UnixHTTPConnection(path=addr, timeout=HTTP_TIMEOUT) as conn:
            return _process_request(conn.make_request(
                method,
                urllib.parse.urljoin("/", url),
                body=body
            ))
    else:
        request = urllib.request.Request(
            urllib.parse.urljoin(f"http://{addr}", url),
            data=body,
            headers={},
            method=method
        )
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as resp:
            return _process_request(resp)


def _process_request(resp) -> HTTPResponse:
    resp_body = resp.read()

    try:
        decoded = resp_body.decode()
    except UnicodeDecodeError:
        return resp_body

    if not (200 <= resp.status <= 300):
        extra = None
        try:
            extra = json.loads(resp_body)
        except ValueError:
            pass
        exc_class = HTTP_EXCEPTIONS.get(resp.status, NSQHTTPException)
        raise exc_class(resp.status, resp_body, extra)

    try:
        response = json.loads(decoded)
    except ValueError:
        return decoded

    return response

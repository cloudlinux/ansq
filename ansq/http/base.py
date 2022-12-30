import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any, Optional, TypeVar

from ansq.typedefs import HTTPResponse
from .http_exceptions import HTTP_EXCEPTIONS, NSQHTTPException
from ..utils import convert_to_bytes

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
        self._endpoint = addr
        self._base_url = "http://{}/".format(self._endpoint)

    @property
    def endpoint(self) -> str:
        return "http://{}".format(self._endpoint)

    async def close(self) -> None:
        pass

    async def perform_request(
        self, method: str, url: str, params: Any, body: Any
    ) -> HTTPResponse:
        _body = convert_to_bytes(body) if body else body

        encoded_params = ""
        if params:
            encoded_params = "?" + urllib.parse.urlencode(params)

        request = urllib.request.Request(
            self._base_url + url + encoded_params,
            data=_body,
            headers={},
            method=method
        )
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _do_request, request)

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f"<{cls_name}: {self._endpoint}>"


def _do_request(request: urllib.request.Request) -> HTTPResponse:
    with urllib.request.urlopen(
        request,
        timeout=HTTP_TIMEOUT
    ) as resp:
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

from datetime import datetime, timezone

from analog.label import (
    ContentType,
    HttpMethod,
    HttpProtocol,
    HttpScheme,
    HttpStatus,
)
from analog.parser import parse_line, to_cool_path


SAFARI_15_6 = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/15.6 Safari/605.1.15"
)


LINES = [
    '1.1.1.1 - - [13/Aug/2022:01:02:03 +0000] "GET / HTTP/2.0"'
    ' 204 0 "-" "bot" s.com 2.2.2.2',
    #
    '3.3.3.3 - - [14/Aug/2022:11:12:13 +0400] "POST /blog/2022/post.html HTTP/1.1"'
    f' 403 665 "https://example.com/some/path" "{SAFARI_15_6}" s.com 2.2.2.2',
]


DATA = [
    {
        "client_address": "1.1.1.1",
        "timestamp": datetime(2022, 8, 13, 1, 2, 3, 0, timezone.utc),
        "method": HttpMethod.GET,
        "path": "/",
        "query": None,
        "protocol": HttpProtocol.HTTP_20,
        "status": 204,
        "size": 0,
        "referrer": None,
        "user_agent": "bot",
        "server_name": "s.com",
        "server_address": "2.2.2.2",
        "content_type": ContentType.MARKUP,
        "cool_path": "/",
        "referrer_scheme": None,
        "referrer_host": None,
        "referrer_path": None,
        "status_class": HttpStatus.SUCCESSFUL,
    },
    {
        "client_address": "3.3.3.3",
        "timestamp": datetime(2022, 8, 14, 7, 12, 13, 0, timezone.utc),
        "method": HttpMethod.POST,
        "path": "/blog/2022/post.html",
        "query": None,
        "protocol": HttpProtocol.HTTP_11,
        "status": 403,
        "size": 665,
        "referrer": "https://example.com/some/path",
        "user_agent": SAFARI_15_6,
        "server_name": "s.com",
        "server_address": "2.2.2.2",
        "content_type": ContentType.MARKUP,
        "cool_path": "/blog/2022/post",
        "referrer_scheme": HttpScheme.HTTPS,
        "referrer_host": "example.com",
        "referrer_path": "/some/path",
        "status_class": HttpStatus.CLIENT_ERROR,
    },
]


def test_parser() -> None:
    for line, data in zip(LINES, DATA):
        assert parse_line(line) == data


def test_to_cool_path() -> None:
    assert to_cool_path("/path/index.html") == "/path"
    assert to_cool_path("/path/to.html") == "/path/to"
    assert to_cool_path("/index.html") == "/"
    assert to_cool_path("") == "/"
    assert to_cool_path("/nothing/changes.xml") == "/nothing/changes.xml"

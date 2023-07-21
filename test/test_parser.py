from datetime import datetime, timezone

from analog.label import (
    ContentType,
    HttpMethod,
    HttpProtocol,
    HttpScheme,
    HttpStatus,
)
from analog.parser import (
    coerce_log_record,
    fill_log_record,
    parse_common_log_format,
    to_cool_path,
)

from analog.schema import ACCESS_LOG_COLUMNS, DERIVED_COLUMNS, NON_NULL_COLUMN_NAMES


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
    #
    '2a06:98c0:3600::103 - - [10/Jan/2023:14:41:04 -0600] '
    '"GET /assets/fonts/bely-regular.woff2 HTTP/2.0" 403 1898 '
    '"-" "-" apparebit.com 192.232.251.218',
]


DATA = [
    {
        "client_address": "1.1.1.1",
        "timestamp": datetime(2022, 8, 13, 1, 2, 3, 0, timezone.utc),
        "method": HttpMethod.GET,
        "path": "/",
        "query": None,
        "fragment": None,
        "protocol": HttpProtocol.HTTP_20,
        "status": 204,
        "size": 0,
        "referrer": None,
        "user_agent": "bot",
        "server_name": "s.com",
        "server_address": "2.2.2.2",
        "cool_path": "/",
        "content_type": ContentType.MARKUP,
        "status_class": HttpStatus.SUCCESSFUL,
        "referrer_scheme": None,
        "referrer_host": None,
        "referrer_path": None,
        "referrer_query": None,
        "referrer_fragment": None,
    },
    {
        "client_address": "3.3.3.3",
        "timestamp": datetime(2022, 8, 14, 7, 12, 13, 0, timezone.utc),
        "method": HttpMethod.POST,
        "path": "/blog/2022/post.html",
        "query": None,
        "fragment": None,
        "protocol": HttpProtocol.HTTP_11,
        "status": 403,
        "size": 665,
        "referrer": "https://example.com/some/path",
        "user_agent": SAFARI_15_6,
        "server_name": "s.com",
        "server_address": "2.2.2.2",
        "cool_path": "/blog/2022/post",
        "content_type": ContentType.MARKUP,
        "status_class": HttpStatus.CLIENT_ERROR,
        "referrer_scheme": HttpScheme.HTTPS,
        "referrer_host": "example.com",
        "referrer_path": "/some/path",
        "referrer_query": None,
        "referrer_fragment": None,
    },
    {
        "client_address": "2a06:98c0:3600::103",
        "timestamp": datetime(2023, 1, 10, 20, 41, 4, 0, timezone.utc),
        "method": HttpMethod.GET,
        "path": "/assets/fonts/bely-regular.woff2",
        "query": None,
        "fragment": None,
        "protocol": HttpProtocol.HTTP_20,
        "status": 403,
        "size": 1898,
        "referrer": None,
        "user_agent": None,
        "server_name": "apparebit.com",
        "server_address": "192.232.251.218",
        "cool_path": "/assets/fonts/bely-regular.woff2",
        "content_type": ContentType.FONT,
        "status_class": HttpStatus.CLIENT_ERROR,
        'referrer_scheme': None,
        'referrer_host': None,
        'referrer_path': None,
        'referrer_query': None,
        "referrer_fragment": None,
    }
]


ACCESS_LOG_COLUMN_NAMES = frozenset(ACCESS_LOG_COLUMNS.keys())
DERIVED_COLUMN_NAMES = frozenset(DERIVED_COLUMNS.keys())


def test_parser() -> None:
    for line, data in zip(LINES, DATA):
        # Parse line. Record fields must be strings or None.
        record = parse_common_log_format(line)
        assert set(record.keys()) == ACCESS_LOG_COLUMN_NAMES
        for key, value in record.items():
            type = str if key in NON_NULL_COLUMN_NAMES else str | None
            assert isinstance(value, type)

        # Coerce fields.
        record = coerce_log_record(record)
        assert set(record.keys()) == ACCESS_LOG_COLUMN_NAMES

        # Add derived fields.
        record = fill_log_record(record)
        assert set(record.keys()) == ACCESS_LOG_COLUMN_NAMES | DERIVED_COLUMN_NAMES
        assert record == data


def test_to_cool_path() -> None:
    assert to_cool_path("/path/index.html") == "/path"
    assert to_cool_path("/path/to.html") == "/path/to"
    assert to_cool_path("/index.html") == "/"
    assert to_cool_path("") == "/"
    assert to_cool_path("/nothing/changes.xml") == "/nothing/changes.xml"

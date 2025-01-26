from __future__ import annotations
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import socket
from typing import Any, cast, Optional, Protocol, TypeAlias

from geoip2.database import Reader as LocationDatabaseReader
from geoip2.errors import AddressNotFoundError
from geoip2.models import City as LocationData

from ua_parser.user_agent_parser import Parse as parse_user_agent

from .atomic_update import atomic_update
from .bot_detector import BotDetector
from .error import ParseError
from .label import (
    ContentType,
    HttpMethod,
    HttpProtocol,
    HttpScheme,
    HttpStatus,
)


class LineParser(Protocol):
    """
    Parse a log line into a dictionary of keyed values. If the log line is
    malformed, the line parser should not throw an exception but rather return
    `None`.
    """
    def __call__(self, line: str) -> dict[str, Any] | None:
        ...

# The log data in columnar format.
LogData: TypeAlias = defaultdict[str, list[Any]]

class Ticker(Protocol):
    """
    A ticker is a callback tracking progress. It is invoked once for each
    processed record.
    """
    def __call__(self) -> None:
        ...

class Pass(Ticker):
    """The trivial ticker that doesn't do anything."""
    def __call__(self) -> None:
        pass

# The canonical do nothing ticker.
PASS = Pass()


# ======================================================================================
# Parse the Raw Log
# ======================================================================================


# Since parts of the log are under control of arbitrary internet users, Apache
# 2.0.49 and later escape octet strings before they are written to the log.
# Space characters, the backslash, and the double quote are written in C-style
# notation: `\\b`, `\\n`, `\\r`, `\\t`, `\\v`, `\\\\`, and `\\"`. Other
# control characters as well as characters with their high bit set are written
# as hexadecimal escapes: `\\xhh`.
#
# While Apache's behavior [is
# documented](https://httpd.apache.org/docs/2.4/mod/mod_log_config.html),
# determining what characters are escaped in what way requires inspecting the
# source code for the
# [ap_escape_logitem](https://github.com/apache/httpd/blob/3e835f22affadfcfa3908277611a0e9961ece1c1/server/util.c#L2204)
# function, which escapes octets before they are logged, and the
# [gen_test_char.c](https://github.com/apache/httpd/blob/5c385f2b6c8352e2ca0665e66af022d6e936db6d/server/gen_test_char.c#L154)
# program, which generates the lookup table containing the `T_ESCAPE_LOGITEM`
# flag for octets that need to be escaped.

_DOUBLE_QUOTED_STRING = r"""
["]
(?:
    \\[bnrtv\\"] # cspell: disable-line
    | \\x[0-9a-fA-F]{2}
    | [^\\"]
)*
["]
"""

_HOST_NAME = r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.?(:\d+)?"
_IP_ADDRESS = (
    r"(?: [0-9]{1,3} (?: [.] [0-9]{1,3})* )"
    r"|(?: [0-9a-fA-F]{1,4} (?: [:][:]? [0-9a-fA-F]{1,4})* )"
)

# Also recognizes combined log format and virtual host
COMMON_LOG_FORMAT = re.compile(
    fr"""
    ^
    (?P<client_address> {_IP_ADDRESS})
    [ ]-[ ]-[ ]
    \[
        (?P<timestamp> [^\]]+)
    \]
    [ ]
    "
        (?P<method> [A-Za-z]+)
        [ ]
        (?P<path> [^?# ]*)
        (?P<query> [?][^# ]*)?
        # There shouldn't be a fragment. But if it's there, let's account for it.
        (?P<fragment> [#][^ ]*)?
        [ ]
        HTTP/(?P<protocol> 0\.9|1\.0|1\.1|2\.0|3\.0)
    "
    [ ]
    (?P<status> \d{{3}})
    [ ]
    (?P<size> - | \d+)
    # Combined Log Format
    (?:
        [ ]
        (?P<referrer> {_DOUBLE_QUOTED_STRING})
        [ ]
        (?P<user_agent> {_DOUBLE_QUOTED_STRING})
    )?
    # Virtual Host
    (?:
        [ ]
        (?P<server_name> {_HOST_NAME})
        [ ]
        (?P<server_address> {_IP_ADDRESS})
    )?
    $
    """,
    re.X,
)


_REFERRER = re.compile(
    r"""
    ^
        (?P<scheme> https?)
        ://
        (?P<host> [^/?#]*)
        (?P<path> [^?#]+)?
        (?P<query> [?][^#]*)?
        (?P<fragment> [#].*)?
    $
    """,
    re.X,
)


def unquote(quoted: str | None) -> Optional[str]:
    """
    Unquote the given double-quoted string. If the argument is `None`, a dash
    `-`, or a double-quoted dash `"-"`, return `None` instead.
    """
    if quoted is None or quoted == '-' or quoted == '"-"':
        return None
    return quoted[1:-1]


def to_cool_path(path: str) -> str:
    """Make the path suitable for inclusion in a cool URL."""
    if path.endswith('/index.html'):
        path = path[:-11]
    elif path.endswith('.html'):
        path = path[:-5]
    if path == '':
        path = '/'
    return path


def parse_common_log_format(line: str) -> dict[str, Any] | None:
    """Parse the fields of a line in common log format."""
    match = COMMON_LOG_FORMAT.match(line)
    return match.groupdict() if match else None


def coerce_log_record(fields: dict[str, Any]) -> dict[str, Any]:
    """Coerce the fields of a log record into expected representation."""
    # client_address unchanged
    fields['timestamp'] = datetime.strptime(
        fields['timestamp'], '%d/%b/%Y:%H:%M:%S %z'
    ).astimezone(timezone.utc)
    fields['method'] = HttpMethod[fields['method'].upper()]
    fields['path'] = path if (path := fields['path']) != '' else '/'
    # query unchanged
    # fragment unchanged
    fields['protocol'] = HttpProtocol(fields['protocol'])
    fields['status'] = int(fields['status'])
    fields['size'] = int(text) if (text := fields['size']) != '-' else 0
    fields['referrer'] = unquote(fields['referrer'])
    fields['user_agent'] = unquote(fields['user_agent'])
    fields['server_name'] = name.lower() if (name := fields['server_name']) else None
    # server_address unchanged
    return fields


def fill_log_record(fields: dict[str, Any]) -> dict[str, Any]:
    """Derrive additional fields with little overhead to simplify analysis."""
    # From path:
    path = fields['path']
    fields['cool_path'] = to_cool_path(path)
    fields['content_type'] = ContentType.of(path)

    # From status:
    fields['status_class'] = HttpStatus.of(fields['status'])

    # From referrer:
    ref = _REFERRER.match(referrer) if (referrer := fields['referrer']) else None
    fields['referrer_scheme'] = HttpScheme(ref.group('scheme').lower()) if ref else None
    fields['referrer_host'] = ref.group('host').lower() if ref else None
    fields['referrer_path'] = ref.group('path') if ref else None
    fields['referrer_query'] = ref.group('query') if ref else None
    fields['referrer_fragment'] = ref.group('fragment') if ref else None

    return fields


def parse_all_lines(
    lines: Iterator[str],
    *,
    parse_line: LineParser = parse_common_log_format,
    ticker: Ticker = PASS,
) -> LogData:
    """Parse all lines in a log."""
    log_data: LogData = defaultdict(list)

    for index, line in enumerate(lines):
        log_record = parse_line(line)
        if log_record is None:
            raise ParseError(f'invalid log line {index + 1} "{line}"')

        log_record = fill_log_record(coerce_log_record(log_record))
        for key, value in log_record.items():
            log_data[key].append(value)

        ticker()

    return log_data


# ======================================================================================
# Enrich Parsed Log
# ======================================================================================


def enrich_client_name(
    log_data: LogData, hostname_db: Path, *, ticker: Ticker = PASS
) -> None:
    try:
        with open(hostname_db, mode='r', encoding='utf8') as file:
            hostnames = json.load(file)
    except FileNotFoundError:
        hostnames = {}

    names = log_data['client_name']
    assert not names

    for address in log_data['client_address']:
        if address not in hostnames:
            try:
                hostnames[address] = socket.gethostbyaddr(address)[0].lower()
            except socket.error:
                hostnames[address] = None

        names.append(hostnames[address])
        ticker()

    with atomic_update(hostname_db) as file:
        json.dump(hostnames, file, indent=0, sort_keys=True)


# --------------------------------------------------------------------------------------


def enrich_client_location(
    log_data: LogData, location_db: Path, *, ticker: Ticker = PASS
) -> None:
    assert not log_data['client_latitude']
    assert not log_data['client_longitude']
    assert not log_data['client_city']
    assert not log_data['client_country']

    def append_to_column(key: str, value: object) -> None:
        log_data[key].append(value)

    with LocationDatabaseReader(os.fspath(location_db)) as reader:
        cache: dict[str, Optional[LocationData]] = dict()

        for address in log_data['client_address']:
            if address not in cache:
                try:
                    cache[address] = reader.city(address)
                except AddressNotFoundError:
                    cache[address] = None

            location = cache[address]
            if location is None:
                append_to_column('client_latitude', math.nan)
                append_to_column('client_longitude', math.nan)
                append_to_column('client_city', None)
                append_to_column('client_country', None)
            else:
                append_to_column('client_latitude', location.location.latitude)
                append_to_column('client_longitude', location.location.longitude)
                append_to_column('client_city', location.city.name)
                append_to_column('client_country', location.country.iso_code)

            ticker()


# --------------------------------------------------------------------------------------


_VERSION_COMPONENTS = ('major', 'minor', 'patch', 'patch_minor')


def enrich_user_agent(log_data: LogData, *, ticker: Ticker = PASS) -> None:
    assert not log_data['agent_family']
    assert not log_data['agent_version']
    assert not log_data['os_family']
    assert not log_data['os_version']
    assert not log_data['device_family']
    assert not log_data['device_brand']
    assert not log_data['device_model']
    assert not log_data['is_bot1']
    assert not log_data['is_bot2']

    def append_to_column(key: str, value: object) -> None:
        log_data[key].append(value)

    bot_detector = BotDetector()

    for user_agent in log_data['user_agent']:
        if user_agent is None:
            append_to_column('agent_family', None)
            append_to_column('agent_version', None)
            append_to_column('os_family', None)
            append_to_column('os_version', None)
            append_to_column('device_family', None)
            append_to_column('device_brand', None)
            append_to_column('device_model', None)
            append_to_column('is_bot1', False)
            append_to_column('is_bot2', False)

            ticker()
            continue

        # FIXME: Consider switching to new ua_parser API
        parts = parse_user_agent(user_agent)
        ua = parts['user_agent']
        os = parts['os']
        device = parts['device']

        append_to_column('agent_family', ua['family'])
        versions = (cast(Optional[str], ua.get(key)) for key in _VERSION_COMPONENTS)
        append_to_column('agent_version', '.'.join(v for v in versions if v))
        append_to_column('os_family', os['family'])
        versions = (cast(Optional[str], os.get(key)) for key in _VERSION_COMPONENTS)
        append_to_column('os_version', '.'.join(v for v in versions if v))
        append_to_column('device_family', device['family'])
        append_to_column('device_brand', device['brand'] or '')
        append_to_column('device_model', device['model'] or '')
        families = (d['family'] for d in (ua, os, device))
        append_to_column('is_bot1', 'Spider' in families and ua['family'] != 'WhatsApp')
        append_to_column('is_bot2', bot_detector.test(user_agent))

        ticker()


# --------------------------------------------------------------------------------------


def enrich(
    log_data: LogData, hostname_db: Path, location_db: Path, ticker: Ticker = PASS
) -> None:
    """
    Enrich the log data by looking up client IP addresses, client location, and
    parsing user agents.
    """
    enrich_client_name(log_data, hostname_db, ticker=ticker)
    enrich_client_location(log_data, location_db, ticker=ticker)
    enrich_user_agent(log_data, ticker=ticker)

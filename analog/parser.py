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
from typing import Any, cast, Optional, TypeAlias

from geoip2.database import Reader as LocationDatabaseReader
from geoip2.errors import AddressNotFoundError
from geoip2.models import City as LocationData

from ua_parser.user_agent_parser import Parse as parse_user_agent

from .atomic_update import atomic_update
from .bot_detector import BotDetector
from .error import ParseError
from .label import (
    BotCategory,
    ContentType,
    HttpMethod,
    HttpProtocol,
    HttpScheme,
    HttpStatus,
)


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
# determining which characters are escaped how requires inspecting the source
# code for the
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
_IP_ADDRESS = r"\d{1,3}(?:\.\d{1,3}){3}"

_LINE_PATTERN = re.compile(
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
    (?P<fragment> [#][^ ]*)?
    [ ]
    HTTP/(?P<protocol> 0\.9|1\.0|1\.1|2\.0|3\.0)
    "
    [ ]
    (?P<status> \d{{3}})
    [ ]
    (?P<size> - | \d+)
    [ ]
    (?P<referrer> {_DOUBLE_QUOTED_STRING})
    [ ]
    (?P<user_agent> {_DOUBLE_QUOTED_STRING})
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


def unquote(quoted: str) -> Optional[str]:
    """
    Unquote the given string. Since the contents of a quoted string are header
    values under user control, this method only removes surrounding double
    quotes but does no further unescaping.
    """
    if quoted == '-' or quoted == '"-"':
        return None
    return quoted[1:-1]


def parse_referrer(
    text: str | None,
) -> tuple[HttpScheme | None, str | None, str | None, str | None, str | None]:
    """Parse the referrer into scheme, host, path, query, and fragment."""
    if text is None or (parts := _REFERRER.match(text)) is None:
        return None, None, None, None, None

    return (
        HttpScheme(parts.group("scheme").lower()),
        parts.group("host").lower(),
        parts.group("path"),
        parts.group("query"),
        parts.group("fragment"),
    )


def parse_size(text: str) -> int:
    """Parse the text as a size value. A dash counts as zero sized."""
    return int(text) if text != "-" else 0


def parse_timestamp(text: str) -> datetime:
    """Parse the text as a timestamp and convert to UTC."""
    timestamp = datetime.strptime(text, "%d/%b/%Y:%H:%M:%S %z")
    return timestamp.astimezone(timezone.utc)


def normalize_path(path: str) -> str:
    """Normalize the path."""
    return path if path else "/"


def to_cool_path(path: str) -> str:
    """Make the path suitable for inclusion in a cool URL."""
    if path.endswith("/index.html"):
        path = path[:-11]
    elif path.endswith(".html"):
        path = path[:-5]
    if path == "":
        path = "/"
    return path


def parse_line(
    line: str, /, derrived_props: bool = True
) -> Optional[dict[str, object]]:
    """
    Parse one log line. This function returns a dictionary with the following
    properties extracted from the line:

      * `client_address`
      * `timestamp`
      * `method`
      * `path`
      * `query`
      * `fragment`
      * `protocol`
      * `status`
      * `size`
      * `referrer`
      * `user_agent`
      * `server_address`
      * `server_name`

    Unless explicitly disabled, it also adds the following convenient derrived
    properties:

      * `content_type`
      * `cool_path`
      * `referrer_scheme`
      * `referrer_host`
      * `referrer_path`
      * `referrer_query`
      * `referrer_fragment`
      * `status_class`

    During extraction, this function:

      * Unquotes double-quoted strings;
      * Converts `size` and `status` to `int`;
      * Converts `timestamp` to `datetime`;
      * Normalizes `method` to upper case;
      * Normalizes an empty path to `/`;
      * Normalizes `referrer_scheme` and `referrer_host` to lower case;
      * Normalizes `server_name` to lower case.

    A well-formed HTTP request should not include a fragment in the path or in
    the referrer. Yet presence of a fragment should only impact log analysis
    that explicitly accounts for it. Hence, this function does parse fragments,
    returning them through the `fragment` and `referrer_fragment` properties.
    """
    if (match := _LINE_PATTERN.match(line)) is None:
        return None

    props = match.groupdict()
    props["method"] = HttpMethod[props["method"].upper()]
    props["protocol"] = HttpProtocol(props["protocol"])
    props["size"] = parse_size(props["size"])
    props["timestamp"] = parse_timestamp(props["timestamp"])
    props["user_agent"] = unquote(props["user_agent"])

    props["path"] = path = normalize_path(props["path"])
    if derrived_props:
        props["content_type"] = ContentType.of(path)
        props["cool_path"] = to_cool_path(path)

    props["referrer"] = referrer = unquote(props["referrer"])
    if derrived_props:
        scheme, host, ref_path, query, fragment = parse_referrer(referrer)
        props["referrer_scheme"] = scheme
        props["referrer_host"] = host
        props["referrer_path"] = ref_path
        props["referrer_query"] = query
        props["referrer_fragment"] = fragment

    props["status"] = status = int(props["status"])
    if derrived_props:
        props["status_class"] = HttpStatus.of(status)

    server_name = props["server_name"]
    props["server_name"] = server_name.lower() if server_name else None

    return props


LogData: TypeAlias = defaultdict[str, list[Any]]


def parse_all_lines(lines: Iterator[str]) -> LogData:
    """Parse all lines in a log."""
    log_data: LogData = defaultdict(list)

    for index, line in enumerate(lines):
        log_entry = parse_line(line)
        if log_entry is None:
            raise ParseError(f'invalid log line {index + 1} "{line}"')

        for key, value in log_entry.items():
            log_data[key].append(value)

    return log_data


# ======================================================================================
# Enrich Parsed Log
# ======================================================================================


def enrich_client_name(log_data: LogData, hostname_db: Path) -> None:
    try:
        with open(hostname_db, mode="r", encoding="utf8") as file:
            hostnames = json.load(file)
    except FileNotFoundError:
        hostnames = {}

    names = log_data["client_name"]
    assert not names

    for address in log_data["client_address"]:
        if address not in hostnames:
            try:
                hostnames[address] = socket.gethostbyaddr(address)[0].lower()
            except socket.error:
                hostnames[address] = None

        names.append(hostnames[address])

    with atomic_update(hostname_db) as file:
        json.dump(hostnames, file, indent=0, sort_keys=True)


# --------------------------------------------------------------------------------------


def enrich_client_location(log_data: LogData, location_db: Path) -> None:
    assert not log_data["client_latitude"]
    assert not log_data["client_longitude"]
    assert not log_data["client_city"]
    assert not log_data["client_country"]

    def append_to_column(key: str, value: object) -> None:
        log_data[key].append(value)

    with LocationDatabaseReader(os.fspath(location_db)) as reader:
        cache: dict[str, Optional[LocationData]] = dict()

        for address in log_data["client_address"]:
            if address not in cache:
                try:
                    cache[address] = reader.city(address)
                except AddressNotFoundError:
                    cache[address] = None

            location = cache[address]
            if location is None:
                append_to_column("client_latitude", math.nan)
                append_to_column("client_longitude", math.nan)
                append_to_column("client_city", None)
                append_to_column("client_country", None)
            else:
                append_to_column("client_latitude", location.location.latitude)
                append_to_column("client_longitude", location.location.longitude)
                append_to_column("client_city", location.city.name)
                append_to_column("client_country", location.country.iso_code)


# --------------------------------------------------------------------------------------


_VERSION_COMPONENTS = ("major", "minor", "patch", "patch_minor")


def enrich_user_agent(log_data: LogData) -> None:
    assert not log_data["agent_family"]
    assert not log_data["agent_version"]
    assert not log_data["os_family"]
    assert not log_data["os_version"]
    assert not log_data["device_family"]
    assert not log_data["device_brand"]
    assert not log_data["device_model"]
    assert not log_data["is_bot"]
    assert not log_data["bot_category"]
    assert not log_data["is_bot2"]

    def append_to_column(key: str, value: object) -> None:
        log_data[key].append(value)

    bot_detector = BotDetector()

    for user_agent in log_data["user_agent"]:
        if user_agent is None:
            append_to_column("agent_family", None)
            append_to_column("agent_version", None)
            append_to_column("os_family", None)
            append_to_column("os_version", None)
            append_to_column("device_family", None)
            append_to_column("device_brand", None)
            append_to_column("device_model", None)
            append_to_column("is_bot", False)
            append_to_column("bot_category", BotCategory.NONE)
            append_to_column("is_bot2", False)
            continue

        parts = parse_user_agent(user_agent)
        ua = parts["user_agent"]
        os = parts["os"]
        device = parts["device"]

        append_to_column("agent_family", ua["family"])
        versions = (cast(Optional[str], ua.get(key)) for key in _VERSION_COMPONENTS)
        append_to_column("agent_version", ".".join(v for v in versions if v))
        append_to_column("os_family", os["family"])
        versions = (cast(Optional[str], os.get(key)) for key in _VERSION_COMPONENTS)
        append_to_column("os_version", ".".join(v for v in versions if v))
        append_to_column("device_family", device["family"])
        append_to_column("device_brand", device["brand"] or "")
        append_to_column("device_model", device["model"] or "")
        is_bot = (
            ua["family"] == "Spider"
            or os["family"] == "Spider"
            or device["family"] == "Spider"
        )
        # Correct misclassification
        if is_bot and ua["family"] == "WhatsApp":
            is_bot = False
        append_to_column("is_bot", is_bot)

        category = BotCategory.of(bot_detector.lookup(user_agent))
        append_to_column("bot_category", category)
        append_to_column("is_bot2", category is not BotCategory.NONE)


# --------------------------------------------------------------------------------------


def enrich(log_data: LogData, hostname_db: Path, location_db: Path) -> None:
    """
    Enrich the log data by looking up client IP addresses, client location, and
    parsing user agents.
    """
    enrich_client_name(log_data, hostname_db)
    enrich_client_location(log_data, location_db)
    enrich_user_agent(log_data)

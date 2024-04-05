from __future__ import annotations
from enum import auto, Enum
import os.path


class EnumLabel(str, Enum):
    """
    The base class of enumerated labels. Thanks to it subclassing from `str` and
    its value being the string representation, enumeration constants deriving
    from this class correctly convert to the corresponding `CategoricalDtype` in
    Pandas.
    """

    @staticmethod
    def _generate_next_value_(
        name: str, _start: int, _count: int, _last_values: list[str]
    ) -> str:
        return name

    def __str__(self) -> str:
        return self.value


class HttpScheme(EnumLabel):
    """HTTP or HTTPS."""

    HTTP = "http"
    HTTPS = "https"


class HttpMethod(EnumLabel):
    """The HTTP method."""

    CONNECT = auto()
    DELETE = auto()
    GET = auto()
    HEAD = auto()
    OPTIONS = auto()
    PATCH = auto()
    POST = auto()
    PUT = auto()
    TRACE = auto()


class HttpProtocol(EnumLabel):
    """The HTTP protocol version."""

    HTTP_09 = "0.9"
    HTTP_10 = "1.0"
    HTTP_11 = "1.1"
    HTTP_20 = "2.0"
    HTTP_30 = "3.0"


class ContentType(EnumLabel):
    """
    The content type. This label does not represent a valid MIME type and hence
    cannot capture the value of the eponymous HTTP header. Its classification
    has a granularity between that of type, subtype pairs and types by
    themselves, leaning closer to the latter extreme.
    """

    CONFIG = auto()
    DIRECTORY = auto()
    FAVICON = auto()
    FONT = auto()
    GRAPHIC = auto()
    IMAGE = auto()
    JSON = auto()
    MARKUP = auto()
    PHP = auto()
    SCRIPT = auto()
    SITEMAP = auto()
    STYLE = auto()
    TEXT = auto()
    UNKNOWN = auto()
    VIDEO = auto()
    XML = auto()

    @staticmethod
    def of(path: str) -> ContentType:
        type = _PATH_2_CONTENT_TYPE.get(path)
        if type:
            return type
        elif path[-1] == "/":
            return ContentType.DIRECTORY

        _, extension = os.path.splitext(path)
        return _EXTENSION_2_CONTENT_TYPE.get(extension, ContentType.UNKNOWN)


_PATH_2_CONTENT_TYPE = {
    "/": ContentType.MARKUP,
    "/favicon.ico": ContentType.FAVICON,
    "/sitemap.xml": ContentType.SITEMAP,
}


# fmt: off
_EXTENSION_2_CONTENT_TYPE = {
    extension: ContentType[content_type]
    for content_type, extensions in [
        ("CONFIG", (".cfg", ".ini", ".webmanifest")),
        ("FONT", (".woff", ".woff2")),
        ("GRAPHIC", (".svg",)),
        ("JSON", (".json",)),
        ("MARKUP", ("", ".htm", ".html")),
        ("IMAGE", (".gif", ".ico", ".jpg", ".jpeg", ".png", ".webp")),
        ("PHP", (".php",)),
        ("SCRIPT", (".js", ".mjs")),
        ("STYLE", (".css",)),
        ("TEXT", (".log", ".txt")),
        ("VIDEO", (".avi", ".mkv", ".mp4")),
        ("XML", (".xml",)),
    ]
    for extension in extensions
}
# fmt: on


APPAREBIT_PAGE_PATHS = [
    '/',
    '/about/apparebit',
    '/about/privacy',
    '/about/robert-grimm',
    '/about/robert-grimm/with-halo',
    '/blog',
    '/blog/2024/netanyahu-plays-goebbels',
    '/blog/2023/great-men',
    '/features/esteban',
    '/blog/2022/new-map-layers',
    '/blog/2022/your-own-kind-of-map',
    '/blog/2022/mapping-package-management',
    '/blog/2022/wrong-impression',
    '/blog/2022/zuckerberg-lies',
    '/blog/2021/lets-try-again',
    '/blog/2021/game-of-thrones',
    '/blog/2019/i-feel-love',
    '/blog/2019/a-frivolous-perspective',
    '/features/breath',
    '/blog/2018/the-donald-is-in-love',
    '/features/do-you-be',
    '/blog/2018/utopia-where-we-find-it',
    '/blog/2018/we-were-grey',
    '/features/ubu-trump',
    '/blog/2016/hopelessness',
    '/features/candy-or-bust',
    '/features/candy-or-bust/voyeur',
]


class HttpStatus(EnumLabel):
    """THE HTTP status class."""

    INFORMATIONAL = auto()
    SUCCESSFUL = auto()
    REDIRECTED = auto()
    CLIENT_ERROR = auto()
    SERVER_ERROR = auto()

    @staticmethod
    def of(status: int) -> HttpStatus:
        if 100 <= status < 200:
            return HttpStatus.INFORMATIONAL
        elif 200 <= status < 300:
            return HttpStatus.SUCCESSFUL
        elif 300 <= status < 400:
            return HttpStatus.REDIRECTED
        elif 400 <= status < 500:
            return HttpStatus.CLIENT_ERROR
        elif 500 <= status < 600:
            return HttpStatus.SERVER_ERROR

        raise ValueError(f"Invalid HTTP status {status}")

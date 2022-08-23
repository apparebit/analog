from analog.label import (
    HttpScheme,
    HttpMethod,
    HttpProtocol,
    ContentType,
    HttpStatus,
)


def test_scheme() -> None:
    assert str(HttpScheme.HTTPS) == "https"
    assert HttpScheme("http") == HttpScheme.HTTP


def test_method() -> None:
    assert str(HttpMethod.GET) == "GET"
    assert HttpMethod("POST") == HttpMethod.POST
    assert HttpMethod["POST"] == HttpMethod.POST


def test_protocol_version() -> None:
    assert str(HttpProtocol.HTTP_09) == "0.9"
    assert HttpProtocol("1.1") == HttpProtocol.HTTP_11
    assert HttpProtocol["HTTP_30"] == HttpProtocol.HTTP_30


def test_content_type() -> None:
    for path, type in [
        ("/", "MARKUP"),
        ("/blog", "MARKUP"),
        ("/blog/index.html", "MARKUP"),
        ("/assets/function.js", "SCRIPT"),
        ("/sitemap.xml", "SITEMAP"),
        ("/assets/form.css", "STYLE"),
        ("/blog/2022/video.avi", "VIDEO"),
    ]:
        assert ContentType.of(path) == ContentType[type]


def test_status() -> None:
    assert HttpStatus.of(200) == HttpStatus.SUCCESSFUL
    assert HttpStatus.of(308) == HttpStatus.REDIRECTION
    assert HttpStatus.of(418) == HttpStatus.CLIENT_ERROR
    assert HttpStatus.of(503) == HttpStatus.SERVER_ERROR

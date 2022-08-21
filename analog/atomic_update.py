import os
import os.path
from tempfile import mkstemp
from types import TracebackType
from typing import cast, Generic, IO, Literal, Optional, TypeVar, overload


# Inspired by
# https://www.edgewall.org/docs/tags-trac-0.11.7/epydoc/trac.util-pysrc.html


T = TypeVar("T", IO[bytes], IO[str])


class atomic_update(Generic[T]):
    """
    Atomically update the file with the given path. A new instance serves as a
    file-like object for a temporary file in the same directory as the given
    path. It also serves a context manager that returns itself. Upon invocation
    of `close()` or regular completion of `with`'s body, the instance atomically
    replaces the given path with the temporary file.
    """

    @overload
    def __init__(
        self: 'atomic_update[IO[str]]',
        path: str | os.PathLike,
        text: Literal[True] = True,
    ) -> None:
        ...

    @overload
    def __init__(
        self: 'atomic_update[IO[bytes]]',
        path: str | os.PathLike,
        text: Literal[False],
    ) -> None:
        ...

    def __init__(self, path: str | os.PathLike, text: bool = True) -> None:
        self._target: str = os.path.realpath(path)
        dir, name = os.path.split(self._target)
        fd, self._tmp = mkstemp(dir=dir, prefix=name + "-", text=text)
        self._file: Optional[T] = os.fdopen(
            fd, mode="w" if text else "wb", encoding="utf8" if text else None
        )
        # Maybe update owner and permissions here?

    def close(self, *, commit: bool = True) -> None:
        if self._file is None:
            return

        try:
            file, self._file = self._file, None
            file.close()
            if commit:
                os.replace(self._tmp, self._target)
        except BaseException:
            # Don't hide underlying exception on abort.
            if commit:
                raise

    def __enter__(self) -> T:
        # Don't expose self._file, since it doesn't implement proper closing
        # behavior. Instead return self, which works thanks to __getattr__.
        return cast(T, self)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close(commit=exc_value is None)

    def __del__(self) -> None:
        self.close(commit=False)

    def __getattr__(self, name: str) -> object:
        return getattr(self._file, name)

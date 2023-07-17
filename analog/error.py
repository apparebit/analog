class AnalogError(Exception):
    """The base class for all of analog's errors."""


class ParseError(AnalogError):
    """An error indicating a malformed access log line."""


class StorageError(AnalogError):
    """An error indicating missing data directories or files."""


class NoFreshCountsError(AnalogError):
    """
    An error indicating that a counting method has been called outside a
    `with analog.fresh_counts()` block.
    """

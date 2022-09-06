class AnalogError(Exception):
    """The base class for all of analog's errors."""


class ParseError(AnalogError):
    """An error indicating a malformed access log line."""


class StorageError(AnalogError):
    """An error indicating missing data directories or files."""


class ValidationError(AnalogError):
    """An error indicating a log dataframe inconsistent with its schema."""

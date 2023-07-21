from __future__ import annotations
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Callable, Generic, TypeAlias, TypeVar

import pandas as pd

from .error import NoFreshCountsError
from .label import ContentType, HttpMethod, HttpStatus
from .month_in_year import monthly_slice

try:
    from IPython.display import display
except ImportError:
    display = print  # type: ignore[assignment]


# --------------------------------------------------------------------------------------
# The global state and context manager for counting


_counts: list[int] | None = None


# --------------------------------------------------------------------------------------
# The base class of all terms


DATA = TypeVar('DATA', pd.Series, pd.DataFrame)
SeriesMapper: TypeAlias = Callable[[pd.DataFrame], pd.Series]


_FrameWrapper = TypeVar('_FrameWrapper', bound='FluentTerm[pd.DataFrame]')


class FluentTerm(Generic[DATA]):
    def __init__(
        self, data: DATA, *, filters: tuple[SeriesMapper,...] | None = None
    ) -> None:
        self._data: DATA = data
        self._filters: tuple[SeriesMapper,...] | None = filters

    @property
    def data(self) -> DATA:
        """
        Unwrap the underlying series or dataframe. Accessing this property
        forces evaluation of any pending filters.
        """
        data = self._data
        filters = self._filters
        if isinstance(data, pd.Series) or filters is None or len(filters) == 0:
            return data

        # There are filters to evaluate! But first we have to appease mypy:
        assert isinstance(data, pd.DataFrame)

        self._filters = None
        if len(filters) == 1:
            self._data = data = data[filters[0](data)]
        else:
            selection = filters[0](data)
            for filter in filters[1:]:
                selection &= filter(data)
            self._data = data = data[selection]
        return data

    def __str__(self) -> str:
        data = self.data
        if isinstance(data, pd.Series):
            return data.to_frame().to_string()
        return data.to_string()

    def _repr_html_(self) -> str | None:
        """Support HTML output in Jupyter notebooks."""
        data = self.data
        if isinstance(data, pd.Series):
            return data.to_frame()._repr_html_() # type: ignore[operator]
        return data._repr_html_() # type: ignore[operator]

    def _filtering(
        self: FluentTerm[pd.DataFrame],
        wrapper: type[_FrameWrapper],
        predicate: SeriesMapper,
    ) -> _FrameWrapper:
        # Delay filter evaluation to avoid intermediate dataframes.
        # Still need to copy filter list before adding predicate.
        fs = self._filters
        fs = (predicate,) if fs is None else (*fs, predicate) # type: ignore[has-type]
        return wrapper(self._data, filters=fs)


# --------------------------------------------------------------------------------------


class FluentSentence(FluentTerm[pd.DataFrame]):
    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Select Rows

    def __getitem__(self, selection: slice) -> FluentSentence:
        """Select rows by their numbers."""
        # Force filter evaluation
        return FluentSentence(self.data[selection])

    @property
    def only(self) -> FluentProtocolSelection:
        """Filter out requests that do not meet the criterion."""
        return FluentProtocolSelection(self._data, filters=self._filters)

    @property
    def over(self) -> FluentRangeSelection:
        """Filter out requests that do not fall into time range."""
        return FluentRangeSelection(self._data, filters=self._filters)

    def filter(self, predicate: SeriesMapper) -> FluentSentence:
        """Lazily apply the given predicate."""
        return self._filtering(FluentSentence, predicate)

    def map(self, mapper: Callable[[pd.DataFrame], pd.DataFrame]) -> FluentSentence:
        """Eagerly apply the given mapping function."""
        # Force filter evaluation
        return FluentSentence(mapper(self.data))

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Compute Statistics Without Rate

    def requests(self) -> int:
        """Return the number of requests."""
        # Force filter evaluation
        return len(self.data)

    def content_types(self) -> FluentDisplay[pd.Series]:
        """
        Determine the number of requests for each content type, producing a
        series.
        """
        return self.value_counts("content_type")

    def status_classes(self) -> FluentDisplay[pd.Series]:
        """
        Determine the number of requests for each status class, producing a
        series.
        """
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> FluentDisplay[pd.Series]:
        """
        Determine the number of requests for each value in the given column,
        producing a series.
        """
        # Force filter evaluation
        return FluentDisplay(
            self.data[column].value_counts().rename_axis(column).rename('count')
        )

    def unique_values(self, column: str) -> FluentDisplay[pd.Series]:
        """
        Determine the unique values in the given column, producing a
        series.
        """
        # Force filter evaluation
        return FluentDisplay(
            self.data[column].drop_duplicates().reindex().rename_axis('row_number')
        )

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Compute Statistics With Rate, i.e., Per Month

    @property
    def monthly(self) -> FluentRate:
        """Compute a monthly breakdown of the data."""
        # Force filter evaluation
        return FluentRate(self.data)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Skip Statistics to Display

    @property
    def just(self) -> FluentDisplay[pd.DataFrame]:
        """Skip to printing or plotting the wrapped dataframe."""
        # Force filter evaluation
        return FluentDisplay(self.data)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Capture Counts

    def count_rows(self) -> FluentSentence:
        """
        Append the number of rows in the wrapped dataframe to the current list
        of counts. This method must be invoked from a `with analog.fresh_counts()`
        block.
        """
        if _counts is None:
            raise NoFreshCountsError(
                'count_rows() called outside `with fresh_counts()` block'
            )

        # Force filter evaluation
        _counts.append(len(self.data))
        return self


# --------------------------------------------------------------------------------------

class FluentProtocolSelection(FluentTerm[pd.DataFrame]):
    def bots(self) -> FluentSentence:
        """
        Select requests made by bots. This method selects requests with user
        agents identified as bots by ua-parser, or matomo, or both.
        """
        return self._filtering(FluentSentence, lambda df: df["is_bot1"] | df["is_bot2"])

    def humans(self) -> FluentSentence:
        """
        Select requests not made by bots. This method selects requests with user
        agents not identified as bots by ua-parser nor matomo.
        """
        return self._filtering(
            FluentSentence, lambda df: (~df["is_bot1"]) & (~df["is_bot2"])
        )

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # The HTTP Methods

    def CONNECT(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.CONNECT)

    def DELETE(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.DELETE)

    def GET(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.GET)

    def HEAD(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.HEAD)

    def OPTIONS(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.OPTIONS)

    def PATCH(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.PATCH)

    def POST(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.POST)

    def PUT(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.PUT)

    def TRACE(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['method'] == HttpMethod.TRACE)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # The Five HTTP Status Classes

    def informational(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['status_class'] == HttpStatus.INFORMATIONAL)

    def successful(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['status_class'] == HttpStatus.SUCCESSFUL)

    def redirected(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['status_class'] == HttpStatus.REDIRECTED)

    def client_error(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['status_class'] == HttpStatus.CLIENT_ERROR)

    def server_error(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['status_class'] == HttpStatus.SERVER_ERROR)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••

    def config(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.CONFIG)

    def directory(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.DIRECTORY)

    def favicon(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.FAVICON)

    def font(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.FONT)

    def graphic(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.GRAPHIC)

    def image(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.IMAGE)

    def json(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.JSON)

    def markup(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.MARKUP)

    def php(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.PHP)

    def script(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.SCRIPT)

    def sitemap(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.SITEMAP)

    def style(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.STYLE)

    def text(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.TEXT)

    def video(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.VIDEO)

    def xml(self) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df['content_type'] == ContentType.XML)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Not Found

    def not_found(self) -> FluentSentence:
        return self._filtering(FluentSentence, lambda df: df["status"] == 404)

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # equals, is_one_of, contains

    def equals(self, column: str, value: object) -> FluentSentence:
        return self._filtering(FluentSentence, lambda df: df[column] == value)

    def is_one_of(
        self, column: str, value1: object, value2: object, *more_values: object
    ) -> FluentSentence:
        values = [value1, value2, *more_values]
        return self._filtering(FluentSentence, lambda df: df[column].isin(values))

    def contains(self, column: str, value: str) -> FluentSentence:
        """
        Filter out all rows that do not contain the given value for the given
        column.
        """
        return self._filtering(
            FluentSentence, lambda df: df[column].str.contains(value)
        )


class FluentRangeSelection(FluentTerm[pd.DataFrame]):
    def _last_period(self, period: str) -> FluentSentence:
        period_object = self._data["timestamp"].max().to_period(period)
        return self.range(period_object.start_time, period_object.end_time)

    def last_day(self) -> FluentSentence:
        return self._last_period('H')

    def last_month(self) -> FluentSentence:
        return self._last_period('M')

    def last_year(self) -> FluentSentence:
        return self._last_period('A')

    def range(
        self, start: datetime | pd.Timestamp, stop: datetime | pd.Timestamp
    ) -> FluentSentence:
        return self._filtering(
            FluentSentence, lambda df: df["timestamp"].between(start, stop)
        )


# --------------------------------------------------------------------------------------


class FluentRate(FluentTerm[pd.DataFrame]):
    def _with_year_month(self) -> tuple[pd.DataFrame, pd.Series]:
        # Copy dataframe before updating it in place.
        df = self.data.copy()
        ts = df['timestamp']
        df.insert(1, 'month', ts.dt.month)
        df.insert(1, 'year', ts.dt.year)
        return df, ts

    def requests(self) -> FluentDisplay[pd.Series]:
        """Count requests per month."""
        df, _ = self._with_year_month()
        return FluentDisplay(
            # Index is labeled multi-index thanks to groupby.
            df.groupby(['year', 'month'])
            .size()
            .rename('requests')
        )

    def content_types(self) -> FluentDisplay[pd.DataFrame]:
        """Determine different content types per month."""
        return self.value_counts("content_type")

    def status_classes(self) -> FluentDisplay[pd.DataFrame]:
        """Determine different status classes per month."""
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> FluentDisplay[pd.DataFrame]:
        """Determine the counts of different values per month."""
        df, ts = self._with_year_month()
        df = df.groupby(['year', 'month', column]).size().unstack(fill_value=0)
        return FluentDisplay(df[monthly_slice(ts)])

    # unique_values() make little sense on a monthly basis.


# --------------------------------------------------------------------------------------


class FluentDisplay(FluentTerm[DATA]):
    def __getitem__(self, selection: slice) -> FluentDisplay[DATA]:
        return type(self)(self.data[selection])

    def format(self) -> list[str]:
        """Format the data, returning the lines of text."""
        return self.data.to_string().splitlines()

    def print(self, *, rows: int | None = None) -> FluentDisplay[DATA]:
        """Print the data. If rows are not None, print only as many rows."""
        data = self.data
        if isinstance(data, pd.Series):
            data = data.to_frame()  # type: ignore[assignment]
        if rows is not None:
            data = data.iloc[:rows]
        display(data)  # type: ignore[no-untyped-call]
        return self

    def plot(self, **kwargs: object) -> FluentDisplay[DATA]:
        """Plot the data."""
        self.data.plot(**kwargs) # type: ignore
        return self

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••

    def count_rows(self) -> FluentDisplay[DATA]:
        """
        Append the number of rows in the wrapped dataframe or series to the
        current list of counts. It error to invoke this method outside a `with
        analog.fresh_counts()` block.
        """
        if _counts is None:
            raise NoFreshCountsError(
                'count_rows() called outside `with fresh_counts()` block'
            )

        # Force filter evaluation
        _counts.append(len(self.data))
        return self

    # ••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Be Done

    def done(self) -> None:
        """
        Be done. This method helps suppress printed values when using a REPL,
        including Jupyter notebook.
        """
        pass


# ======================================================================================


def unwrapped(value: FluentTerm[DATA] | DATA) -> DATA:
    """Return a definitely unwrapped dataframe or series."""
    return value.data if isinstance(value, FluentTerm) else value


def analyze(data: FluentTerm[pd.DataFrame] | pd.DataFrame) -> FluentSentence:
    """
    Analyze the dataframe. This function returns the wrapped dataframe, ready
    for fluent processing.
    """
    return FluentSentence(unwrapped(data))


def merge(
    *series: FluentTerm[pd.Series] | pd.Series,
    **named_series: FluentTerm[pd.Series] | pd.Series,
) -> FluentSentence:
    """
    Merge the series as columns in a new, wrapped dataframe. Positional
    arguments are unwrapped only. Keyword arguments are unwrapped and renamed
    with the key.
    """
    full_series = [unwrapped(s) for s in series]
    full_series.extend(unwrapped(s).rename(n) for n, s in named_series.items())
    return FluentSentence(pd.concat(full_series, axis=1))


@contextmanager
def fresh_counts() -> Iterator[list[int]]:
    """
    Install a new list for capturing counts generated by fluent `.count_rows()`
    method invocations. You access the list by binding the value of the context
    manager in the `with` statement.
    """
    global _counts
    old_counts, _counts = _counts, []
    try:
        yield _counts
    finally:
        _counts = old_counts

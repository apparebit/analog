from __future__ import annotations
from datetime import datetime
from typing import Callable, Generic, Optional, TypeAlias, TypeVar, overload

import pandas as pd

from .label import ContentType, HttpMethod, HttpProtocol, HttpStatus
from .month_in_year import monthly_slice


_DT = TypeVar('_DT', pd.Series, pd.DataFrame)
_FrameWrapper = TypeVar('_FrameWrapper', bound='_FluentTerm[pd.DataFrame]')
_SeriesMapper: TypeAlias = Callable[[pd.DataFrame], pd.Series]


class _FluentTerm(Generic[_DT]):
    def __init__(self, data: _DT, filters: list[_SeriesMapper]) -> None:
        self._data: _DT = data
        self._filters = filters

    @property
    def data(self) -> _DT:
        """
        Unwrap the underlying series or dataframe. Accessing this property
        forces evaluation of any pending filters.
        """
        data = self._data
        filters = self._filters
        if isinstance(data, pd.Series) or len(filters) == 0:
            return data

        # There are filters to evaluate! But first we have to appease mypy:
        assert isinstance(data, pd.DataFrame)

        self._filters = []
        if len(filters) == 1:
            self._data = data = data[filters[0](data)]
        else:
            selection = filters[0](data)
            for filter in filters[1:]:
                selection &= filter(data)
            self._data = data = data[selection]
        return data

    def handoff(
        self: _FluentTerm[pd.DataFrame], wrapper: type[_FrameWrapper]
    ) -> _FrameWrapper:
        return wrapper(self._data, self._filters)

    def filter(
        self: _FluentTerm[pd.DataFrame],
        wrapper: type[_FrameWrapper],
        predicate: _SeriesMapper,
    ) -> _FrameWrapper:
        # Delay filter evaluation to avoid plethora of intermediate dataframes.
        return wrapper(self._data, [*self._filters, predicate])


# --------------------------------------------------------------------------------------


class _FluentSentence(_FluentTerm[pd.DataFrame]):
    # •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Filters

    @property
    def only(self) -> _FluentFilter:
        """Filter out requests that do not meet the criterion."""
        return self.handoff(_FluentFilter)

    @property
    def over(self) -> _FluentRange:
        """Filter out requests that do not fall into time range."""
        return self.handoff(_FluentRange)

    # •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # No Counts

    @property
    def as_is(self) -> _FluentDisplay[pd.DataFrame]:
        # Force filter evaluation
        return _FluentDisplay(self.data, [])

    # •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Counts

    def requests(self) -> int:
        # Force filter evaluation
        return len(self.data)

    def content_types(self) -> _FluentDisplay[pd.Series]:
        return self.value_counts("content_type")

    def status_classes(self) -> _FluentDisplay[pd.Series]:
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> _FluentDisplay[pd.Series]:
        # Force filter evaluation
        return _FluentDisplay(self.data[column].value_counts(), [])

    def unique_values(self, column: str) -> _FluentDisplay[pd.Series]:
        # Force filter evaluation
        return _FluentDisplay(self.data[column].drop_duplicates(), [])

    # •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # Monthly Counts

    @property
    def monthly(self) -> _FluentRate:
        """Compute monthly breakdown of a column."""
        # Force filter evaluation
        return _FluentRate(self.data, [])

    # •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
    # For observability/debugability

    def quantify(
        self, rows: list[int], columns: list[int] | None = None
    ) -> _FluentSentence:
        """Add size of wrapped dataframe to given counts."""
        rows.append(len(self.data))
        if columns is not None:
            columns.append(len(self.data.columns))
        return self


# --------------------------------------------------------------------------------------


class _FluentFilter(_FluentTerm[pd.DataFrame]):
    def bots(self) -> _FluentSentence:
        """
        Select requests made by bots. This method selects requests with user
        agents identified as bots by ua-parser, or matomo, or both.
        """
        return self.filter(_FluentSentence, lambda df: df["is_bot"] | df["is_bot2"])

    def humans(self) -> _FluentSentence:
        """
        Select requests not made by bots. This method selects requests with user
        agents not identified as bots by ua-parser nor matomo.
        """
        return self.filter(
            _FluentSentence, lambda df: (~df["is_bot"]) & (~df["is_bot2"])
        )

    def GET(self) -> _FluentSentence:
        return self.filter(_FluentSentence, lambda df: df["method"] == HttpMethod.GET)

    def POST(self) -> _FluentSentence:
        return self.filter(_FluentSentence, lambda df: df["method"] == HttpMethod.POST)

    def markup(self) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["content_type"] == ContentType.MARKUP
        )

    def successful(self) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["status_class"] == HttpStatus.SUCCESSFUL
        )

    def redirection(self) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["status_class"] == HttpStatus.REDIRECTION
        )

    def client_error(self) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["status_class"] == HttpStatus.CLIENT_ERROR
        )

    def not_found(self) -> _FluentSentence:
        return self.filter(_FluentSentence, lambda df: df["status"] == 404)

    def server_error(self) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["status_class"] == HttpStatus.SERVER_ERROR
        )

    _COLUMNS = {
        ContentType: "content_type",
        HttpMethod: "method",
        HttpProtocol: "protocol",
        HttpStatus: "status_class",
    }

    def has(
        self, criterion: ContentType | HttpMethod | HttpProtocol | HttpStatus
    ) -> _FluentSentence:
        """
        Filter out all rows that do not match the given criterion. The column to
        filter is automatically determined based on the type of criterion. For
        that reason, this method only handles columns with categorical types.
        """
        column = _FluentFilter._COLUMNS[type(criterion)]
        return self.filter(_FluentSentence, lambda df: df[column] == criterion)

    def equals(self, column: str, value: object) -> _FluentSentence:
        """
        Filter out all rows that do not have the given value for the given
        column. This method generalizes `has()` for columns that are not
        categorical.
        """
        return self.filter(_FluentSentence, lambda df: df[column] == value)

    def contains(self, column: str, value: str) -> _FluentSentence:
        """
        Filter out all rows that do not contain the given value for the given
        column.
        """
        return self.filter(_FluentSentence, lambda df: df[column].str.contains(value))


class _FluentRange(_FluentTerm[pd.DataFrame]):
    def last_day(self) -> _FluentSentence:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(days=1), latest)

    def last_month(self) -> _FluentSentence:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(months=1), latest)

    def last_year(self) -> _FluentSentence:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(years=1), latest)

    def range(
        self, start: datetime | pd.Timestamp, stop: datetime | pd.Timestamp
    ) -> _FluentSentence:
        return self.filter(
            _FluentSentence, lambda df: df["timestamp"].between(start, stop)
        )


# --------------------------------------------------------------------------------------


class _FluentRate(_FluentTerm[pd.DataFrame]):
    def _with_year_month(self) -> tuple[pd.DataFrame, pd.Series]:
        # Copy dataframe before updating it in place.
        df = self.data.copy()
        ts = df['timestamp']
        df.insert(1, 'month', ts.dt.month)
        df.insert(1, 'year', ts.dt.year)
        return df, ts

    def requests(self) -> _FluentDisplay[pd.Series]:
        df, _ = self._with_year_month()
        return _FluentDisplay(df.groupby(['year', 'month']).size(), [])

    def content_types(self) -> _FluentDisplay[pd.DataFrame]:
        return self.value_counts("content_type")

    def status_classes(self) -> _FluentDisplay[pd.DataFrame]:
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> _FluentDisplay[pd.DataFrame]:
        df, ts = self._with_year_month()
        df = df.groupby(['year', 'month', column]).size().unstack(fill_value=0)
        return _FluentDisplay(df[monthly_slice(ts)], [])

    def unique_values(self, column: str) -> _FluentDisplay[pd.DataFrame]:
        raise NotImplementedError()  # Oops!


# --------------------------------------------------------------------------------------


class _FluentDisplay(_FluentTerm[_DT]):
    def then_format(self) -> list[str]:
        return self.data.to_string().splitlines()

    def then_print(self, rows: Optional[int] = None) -> _FluentDisplay[_DT]:
        if rows is None:
            print(self.data.to_string())
        else:
            print(self.data.head(rows))
        return self

    def then_plot(self, **kwargs: object) -> _FluentDisplay[_DT]:
        self.data.plot(**kwargs)
        return self

    def quantify(
        self, rows: list[int], columns: list[int] | None = None
    ) -> _FluentDisplay[_DT]:
        """Add size of wrapped series or dataframe to given counts."""
        rows.append(len(self.data))
        if columns is not None:
            if isinstance(self.data, pd.Series):
                columns.append(1)
            else:
                columns.append(len(self.data.columns))
        return self


# ======================================================================================


def analyze(frame: pd.DataFrame) -> _FluentSentence:
    """
    Analyze the dataframe. This function returns the wrapped dataframe, ready
    for fluent processing.
    """
    return _FluentSentence(frame, [])


def merge(columns: dict[str, _FluentTerm[pd.Series]]) -> _FluentSentence:
    """Merge the named series as columns in a new, wrapped dataframe."""
    return _FluentSentence(
        pd.concat([s.data.copy().rename(n) for n, s in columns.items()], axis=1), []
    )

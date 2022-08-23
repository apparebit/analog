from __future__ import annotations
from typing import Any, Callable, Generic, Optional, TypeAlias, TypeVar

import pandas as pd

from .data_manager import Coverage
from .label import ContentType, HttpMethod, HttpProtocol, HttpStatus


DT = TypeVar("DT", pd.Series, pd.DataFrame)
FrameWrapper = TypeVar("FrameWrapper", bound="FluentTerm[pd.DataFrame]")

FrameMapper: TypeAlias = Callable[[pd.DataFrame], pd.DataFrame]
SeriesMapper: TypeAlias = Callable[[pd.DataFrame], pd.Series]


class FluentTerm(Generic[DT]):
    def __init__(self, data: DT, cover: Coverage) -> None:
        self._data: DT = data
        self._cover: Coverage = cover

    @property
    def data(self) -> DT:
        return self._data

    @property
    def coverage(self) -> Coverage:
        return self._cover

    def handoff(
        self: FluentTerm[pd.DataFrame], wrapper: type[FrameWrapper]
    ) -> FrameWrapper:
        return wrapper(self._data, self._cover)

    def map(
        self: FluentTerm[pd.DataFrame], wrapper: type[FrameWrapper], mapper: FrameMapper
    ) -> FrameWrapper:
        return wrapper(mapper(self._data), self._cover)

    def filter(
        self: FluentTerm[pd.DataFrame],
        wrapper: type[FrameWrapper],
        predicate: SeriesMapper,
    ) -> FrameWrapper:
        return wrapper(self._data[predicate(self._data)], self._cover)


class FluentSentence(FluentTerm[pd.DataFrame]):
    @property
    def only(self) -> FluentFilter:
        """Filter out requests that do not meet the criterion."""
        return self.handoff(FluentFilter)

    @property
    def monthly(self) -> FluentRate:
        """Compute monthly breakdown of a column."""
        return self.handoff(FluentRate)

    @property
    def over(self) -> FluentRange:
        """Filter out requests older than a month or a year."""
        return self.handoff(FluentRange)

    @property
    def as_is(self) -> FluentDisplay[pd.DataFrame]:
        return self.handoff(FluentDisplay)


class FluentFilter(FluentTerm[pd.DataFrame]):
    def bots(self) -> FluentSentence:
        return self.filter(FluentSentence, lambda df: df["is_bot"])

    def humans(self) -> FluentSentence:
        return self.filter(FluentSentence, lambda df: ~df["is_bot"])

    def get(self) -> FluentSentence:
        return self.filter(FluentSentence, lambda df: df["method"] == HttpMethod.GET)

    def post(self) -> FluentSentence:
        return self.filter(FluentSentence, lambda df: df["method"] == HttpMethod.POST)

    def markup(self) -> FluentSentence:
        return self.filter(
            FluentSentence, lambda df: df["content_type"] == ContentType.MARKUP
        )

    def successful(self) -> FluentSentence:
        return self.filter(
            FluentSentence, lambda df: df["status_class"] == HttpStatus.SUCCESSFUL
        )

    def redirection(self) -> FluentSentence:
        return self.filter(
            FluentSentence, lambda df: df["status_class"] == HttpStatus.REDIRECTION
        )

    def client_error(self) -> FluentSentence:
        return self.filter(
            FluentSentence, lambda df: df["status_class"] == HttpStatus.CLIENT_ERROR
        )

    def not_found(self) -> FluentSentence:
        return self.filter(FluentSentence, lambda df: df["status"] == 404)

    def server_error(self) -> FluentSentence:
        return self.filter(
            FluentSentence, lambda df: df["status_class"] == HttpStatus.SERVER_ERROR
        )

    _COLUMNS = {
        ContentType: "content_type",
        HttpMethod: "method",
        HttpProtocol: "protocol",
        HttpStatus: "status_class",
    }

    def having(
        self, criterion: ContentType | HttpMethod | HttpProtocol | HttpStatus
    ) -> FluentSentence:
        """
        Filter out all rows that do not match the given criterion. The column to
        filter is automatically determined based on the type of criterion. For
        that reason, this method only handles columns with categorical types.
        """
        column = FluentFilter._COLUMNS[type(criterion)]
        return self.filter(FluentSentence, lambda df: df[column] == criterion)


class FluentRate(FluentTerm[pd.DataFrame]):
    def requests(self) -> FluentDisplay[pd.Series]:
        ts = self._data["timestamp"]
        return FluentDisplay(
            self._data.groupby([ts.dt.year, ts.dt.month]).size(),  # type: ignore
            self._cover,
        )

    def content_types(self) -> FluentDisplay[pd.DataFrame]:
        return self.value_counts("content_type")

    def status_classes(self) -> FluentDisplay[pd.DataFrame]:
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> FluentDisplay[pd.DataFrame]:
        ts = self._data["timestamp"]
        df = (
            self._data.groupby([ts.dt.year, ts.dt.month, column])  # type: ignore
            .size()
            .unstack(fill_value=0)
        )
        return FluentDisplay(
            df[self._cover.begin : self._cover.end], self._cover  # type: ignore
        )


class FluentRange(FluentTerm[pd.DataFrame]):
    def lifetime(self) -> FluentStatistic:
        return self.handoff(FluentStatistic)

    def last_day(self) -> FluentStatistic:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(days=1), latest)

    def last_month(self) -> FluentStatistic:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(months=1), latest)

    def last_year(self) -> FluentStatistic:
        latest = self._data["timestamp"].max()
        return self.range(latest - pd.DateOffset(years=1), latest)

    def range(self, begin: pd.Timestamp, end: pd.Timestamp) -> FluentStatistic:
        return self.filter(
            FluentStatistic, lambda df: df["timestamp"].between(begin, end)
        )


class FluentStatistic(FluentTerm[pd.DataFrame]):
    def requests(self) -> int:
        return len(self._data)

    def content_types(self) -> FluentDisplay[pd.Series]:
        return self.value_counts("content_type")

    def status_classes(self) -> FluentDisplay[pd.Series]:
        return self.value_counts("status_class")

    def value_counts(self, column: str) -> FluentDisplay[pd.Series]:
        return FluentDisplay(self._data[column].value_counts(), self._cover)


class FluentDisplay(FluentTerm[DT]):
    def then_print(self, rows: Optional[int] = None) -> FluentDisplay[DT]:
        if rows is None:
            print(self._data.to_string())
        else:
            print(self._data.head(rows))
        return self

    def then_plot(self, **kwargs: object) -> FluentDisplay[DT]:
        self._data.plot(**kwargs)
        return self


# ======================================================================================


def analyze(frame: pd.DataFrame, cover: Coverage) -> FluentSentence:
    """
    Analyze the dataframe and its coverage. This function returns the wrapped
    dataframe and coverage, ready for fluent processing.
    """
    return FluentSentence(frame, cover)


def merge(columns: dict[str, FluentTerm[pd.Series]]) -> FluentSentence:
    """
    Merge the named series as columns in a new, wrapped dataframe. All series
    must have the same coverage, i.e., be derrived from the same original
    dataframe.
    """
    for index, wrapper in enumerate(columns.values()):
        if index == 0:
            cover = wrapper.coverage
        elif cover is not wrapper.coverage:
            raise ValueError(f'Series {columns} have different coverage')

    return FluentSentence(
        pd.concat([s.data.rename(n) for n, s in columns.items()], axis=1), cover
    )

from __future__ import annotations
from typing import Any, Callable, Optional, TypeVar

import pandas as pd

from .data_manager import Coverage
from .label import ContentType, HttpMethod, HttpStatus, HttpStatus


SeriesWrapper = TypeVar('SeriesWrapper', bound='Seriesful')
FrameWrapper = TypeVar('FrameWrapper', bound='Frameful')

SeriesMapper = Callable[[pd.DataFrame], pd.Series]
FrameMapper = Callable[[pd.DataFrame], pd.DataFrame]


class Seriesful:
    def __init__(self, series: pd.Series, cover: Coverage) -> None:
        self._series = series
        self._cover = cover

    @property
    def series(self) -> pd.Series:
        return self._series

    @property
    def coverage(self) -> Coverage:
        return self._cover


class FluentSeriesDisplay(Seriesful):
    def then_print(self, entries: Optional[int] = None) -> FluentSeriesDisplay:
        if entries is not None:
            print(self._series.head(entries))
        else:
            print(self._series.to_string())
        return self

    def then_plot(self, **kwargs: object) -> FluentSeriesDisplay:
        self._series.plot(**kwargs)
        return self


# ======================================================================================


class Frameful:
    def __init__(self, frame: pd.DataFrame, cover: Coverage) -> None:
        self._frame = frame
        self._cover = cover

    @property
    def frame(self) -> pd.DataFrame:
        return self._frame

    @property
    def coverage(self) -> Coverage:
        return self._cover

    def handoff(self, wrapper: type[FrameWrapper]) -> FrameWrapper:
        return wrapper(self._frame, self._cover)

    def map(self, wrapper: type[FrameWrapper], mapper: FrameMapper) -> FrameWrapper:
        return wrapper(mapper(self._frame), self._cover)

    def filter(
        self, wrapper: type[FrameWrapper], predicate: SeriesMapper
    ) -> FrameWrapper:
        return wrapper(self._frame[predicate(self._frame)], self._cover)

    def serialize(
        self, wrapper: type[SeriesWrapper], mapper: SeriesMapper
    ) -> SeriesWrapper:
        return wrapper(mapper(self._frame), self._cover)


# --------------------------------------------------------------------------------------
# Normally, FluentPhrase would be defined here. It starts fluent phrases after
# all. Alas, it allows skipping straight to display and hence is defined at end
# of module.


class FluentFilter(Frameful):
    def bots(self) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: df["is_bot"])

    def humans(self) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: ~df["is_bot"])

    def get(self) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: df["method"] == HttpMethod.GET)

    def post(self) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: df["method"] == HttpMethod.POST)

    # FIXME awkward method name
    def has(self, content_type: ContentType) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: df["content_type"] == content_type)

    def markup(self) -> FluentPhrase:
        return self.filter(
            FluentPhrase, lambda df: df["content_type"] == ContentType.MARKUP
        )

    def successful(self) -> FluentPhrase:
        return self.filter(
            FluentPhrase, lambda df: df["status_class"] == HttpStatus.SUCCESSFUL
        )

    def redirection(self) -> FluentPhrase:
        return self.filter(
            FluentPhrase, lambda df: df["status_class"] == HttpStatus.REDIRECTION
        )

    def client_error(self) -> FluentPhrase:
        return self.filter(
            FluentPhrase, lambda df: df["status_class"] == HttpStatus.CLIENT_ERROR
        )

    def not_found(self) -> FluentPhrase:
        return self.filter(FluentPhrase, lambda df: df["status"] == 404)

    def server_error(self) -> FluentPhrase:
        return self.filter(
            FluentPhrase, lambda df: df["status_class"] == HttpStatus.SERVER_ERROR
        )


class FluentDuration(Frameful):
    def last_month(self) -> FluentBreakdown:
        one_month_ago = self._frame["timestamp"].max() - pd.DateOffset(months=1)
        return self.filter(FluentBreakdown, lambda df: df["timestamp"] > one_month_ago)

    def last_year(self) -> FluentBreakdown:
        one_year_ago = self._frame["timestamp"].max() - pd.DateOffset(years=1)
        return self.filter(FluentBreakdown, lambda df: df["timestamp"] > one_year_ago)


class FluentBreakdown(Frameful):
    # FIXME: Need a way to bypass by(); rename by()
    def by(self, column: str) -> FluentSeriesDisplay:
        return self.serialize(FluentSeriesDisplay, lambda df: df[column].value_counts())


class FluentRequestRate(Frameful):
    def per_day(self) -> FluentSeriesDisplay:
        return self.serialize(
            FluentSeriesDisplay,
            lambda df: df.resample('D', on='timestamp').size(),  # type: ignore [no-untyped-call]
        )

    def per_month(self) -> FluentSeriesDisplay:
        return self.serialize(
            FluentSeriesDisplay,
            lambda df: df.resample('MS', on='timestamp').size(),  # type: ignore [no-untyped-call]
        )


class FluentFrameDisplay(Frameful):
    def then_print(self, entries: Optional[int] = None) -> FluentFrameDisplay:
        if entries is not None:
            print(self._frame.head(entries))
        else:
            print(self._frame.to_string())
        return self

    def then_plot(self, **kwargs: object) -> FluentFrameDisplay:
        self._frame.plot(**kwargs)
        return self


class FluentPhrase(FluentFrameDisplay):
    @property
    def only(self) -> FluentFilter:
        """Filter out requests that do not meet the criterion."""
        return self.handoff(FluentFilter)

    @property
    def during(self) -> FluentDuration:
        """Filter out requests older than a month or a year."""
        return self.handoff(FluentDuration)

    @property
    def as_requests(self) -> FluentRequestRate:
        """Convert to requests per month or day."""
        return self.handoff(FluentRequestRate)


# ======================================================================================


def analyze(frame: pd.DataFrame, cover: Coverage) -> FluentPhrase:
    return FluentPhrase(frame, cover)


def merge(columns: dict[str, Seriesful]) -> FluentPhrase:
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

    return FluentPhrase(
        pd.concat([s.series.rename(n) for n, s in columns.items()], axis=1), cover
    )

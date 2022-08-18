from __future__ import annotations
from typing import Any

import pandas as pd

from .label import ContentType, HttpMethod, HttpStatus, HttpStatus


def calculate(df: pd.DataFrame) -> FrameAnalyzer:
    """
    Turn the dataframe into a fluent log analyzer. To simplify analysis, this
    function updates the dataframe's index to its `timestamp` column. It does so
    destructively, in place.
    """
    if "timestamp" in df.columns:
        df.set_index("timestamp", inplace=True)
    return FrameAnalyzer(df)


class FrameAnalyzer:
    def __init__(self, df: pd.DataFrame) -> None:
        self._frame = df

    @property
    def frame(self) -> pd.DataFrame:
        return self._frame

    # ----------------------------------------------------------------------------------
    # Subject to denote bots or humans

    def bots(self) -> FrameAnalyzer:
        """Accept requests from bots only."""
        return FrameAnalyzer(self._frame[self._frame["is_bot"]])

    def humans(self) -> FrameAnalyzer:
        """Accept requests from humans only."""
        return FrameAnalyzer(self._frame[~self._frame["is_bot"]])

    # ----------------------------------------------------------------------------------
    # Adverb to denote the rough outcome

    def successfully(self) -> FrameAnalyzer:
        """
        Accept successful requests with a 2xx status code indicating
        successful completion only.
        """
        return FrameAnalyzer(
            self._frame[self._frame["status_class"] == HttpStatus.SUCCESSFUL]
        )

    def inaccurately(self) -> FrameAnalyzer:
        """
        Accept requests with a 3xx status code indicating redirection only.
        """
        return FrameAnalyzer(
            self._frame[self._frame["status_class"] == HttpStatus.REDIRECTION]
        )

    def incorrectly(self) -> FrameAnalyzer:
        """
        Accept incorrect requests with a 4xx status code indicating a client
        error only.
        """
        return FrameAnalyzer(
            self._frame[self._frame["status_class"] == HttpStatus.CLIENT_ERROR]
        )

    def unsuccessfully(self) -> FrameAnalyzer:
        """
        Accept unsuccessful requests with a 5xx status code indicating server
        error only.
        """
        return FrameAnalyzer(
            self._frame[self._frame["status_class"] == HttpStatus.SERVER_ERROR]
        )

    # ----------------------------------------------------------------------------------
    # Verb to denote the HTTP method

    def getting(self) -> FrameAnalyzer:
        """Filter out requests with methods other than GET."""
        return FrameAnalyzer(
            self._frame[self._frame["method"] == HttpMethod.GET]
        )

    def posting(self) -> FrameAnalyzer:
        """Filter out requests with methods other than POST."""
        return FrameAnalyzer(
            self._frame[self._frame["method"] == HttpMethod.POST]
        )

    # ----------------------------------------------------------------------------------
    # Optional adjective to denote specific outcome

    def non_existent(self) -> FrameAnalyzer:
        """Accept only requests resulting in a 404 Not Found status."""
        return FrameAnalyzer(
            self._frame[self._frame["status"] == 404]
        )

    # ----------------------------------------------------------------------------------
    # Object to denote the type of content

    def resources(self) -> FrameAnalyzer:
        """Accept all content."""
        return self

    def content(self, ct: ContentType) -> FrameAnalyzer:
        """Filter out content other than the given content type."""
        return FrameAnalyzer(
            self._frame[self._frame["content_type"] == ct.value]
        )

    def markup(self) -> FrameAnalyzer:
        """Filter out content other than HTML markup."""
        return FrameAnalyzer(
            self._frame[self._frame["content_type"] == ContentType.MARKUP]
        )

    # ----------------------------------------------------------------------------------
    # Adverb to denote calculation frequency

    def per_month(self) -> SeriesAnalyzer:
        """Count the number of requests per month as a time series (not frame)."""
        # M is end of month and MS is start of month.
        return SeriesAnalyzer(
            self._frame.resample("MS").size()  # type: ignore[no-untyped-call]
        )

    def per_day(self) -> SeriesAnalyzer:
        """Count the number of requests per day as a time series (not frame)."""
        return SeriesAnalyzer(
            self._frame.resample("D").size()  # type: ignore[no-untyped-call]
        )

    # ----------------------------------------------------------------------------------
    # New sentence to denote output

    def then_print(self) -> FrameAnalyzer:
        """Print the result to standard output."""
        print(self._frame.to_string())
        return self

    def then_plot(self, **kwargs: object) -> FrameAnalyzer:
        """Plot the result."""
        self._frame.plot(**kwargs)
        return self


class SeriesAnalyzer:
    def __init__(self, series: pd.Series) -> None:
        self._series = series

    @property
    def series(self) -> pd.Series:
        return self._series

    def then_plot(self) -> SeriesAnalyzer:
        self._series.plot()
        return self



#     ts = df[timestamp]
#     ts_min = ts.min()
#     ts_max = ts.max()
#     result = df.groupby([ts.dt.year, ts.dt.month, third_column]).aggregate(aggregate)
#     result = result[
#         (ts_min.year, ts_min.month, sc_min):(ts_max.year, ts_max.month, sc_max)
#     ]

# Metrics:
#
# * Total number of requests
# * Requests broken down by status class for humans vs bots
# * Requests broken down by content type for humans vs bots
# * Views per page, i.e., successful non-bot GETs for markup per cool path.
# * The same for all content
# * Resources that were not found by human users, sorted by popularity.
# * Referrals

from __future__ import annotations
from calendar import monthrange
from datetime import timezone
import re
from typing import NamedTuple

import pandas as pd


_SHORT_MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)

_YYYY_MM = re.compile(r"\d\d\d\d[-./_]\d\d")


class MonthInYear(NamedTuple):
    """A specific month in a specific year."""

    year: int
    month: int

    @classmethod
    def from_mmm_yyyy(cls, text: str) -> MonthInYear:
        """
        Create month-in-year from first three letters of month, separator
        character, and four digit year.
        """
        assert len(text) == 8

        year = int(text[4:])
        month = _SHORT_MONTHS.index(text[:3]) + 1
        return cls(year=year, month=month)

    @staticmethod
    def is_yyyy_mm(text: str) -> bool:
        """Determine whether text is in YYYY-MM format."""
        if len(text) != 7:
            return False
        return bool(_YYYY_MM.fullmatch(text))

    @classmethod
    def from_yyyy_mm(cls, text: str) -> MonthInYear:
        """
        Create month-in-year from four digit year, separator character, and two
        digit month.
        """
        assert len(text) == 7

        year = int(text[:4])
        month = int(text[-2:])
        return cls(year=year, month=month)

    @classmethod
    def from_timestamp(cls, timestamp: pd.Timestamp) -> MonthInYear:
        """Create month-in-year from Pandas timestamp."""
        return cls(year=timestamp.year, month=timestamp.month)

    def days(self) -> int:
        """Get number of days for this month in year."""
        _, days = monthrange(self.year, self.month)
        return days

    def __str__(self) -> str:
        return f"{self.year:04}-{self.month:02}"

    def __sub__(self, other: MonthInYear) -> int:
        return (self.year - other.year) * 12 + (self.month - other.month)

    def previous(self) -> MonthInYear:
        """Determine the previous month-in-year."""
        year = self.year
        month = self.month - 1
        if month == 0:
            year -= 1
            month = 12
        return self.__class__(year=year, month=month)

    def next(self) -> MonthInYear:
        """Determine the next month-in-year."""
        year = self.year
        month = self.month + 1
        if month == 13:
            year += 1
            month = 1
        return self.__class__(year=year, month=month)

    @property
    def start(self) -> pd.Timestamp:
        """
        Determine the first moment for this month-in-year. The resulting
        timestamp uses UTC as timezone.
        """
        return pd.Timestamp(str(self), tz=timezone.utc)

    @property
    def stop(self) -> pd.Timestamp:
        """
        Determine the last moment for this month-in-year. The resulting
        timestamp has nanosecond resolution and uses UTC as timezone.
        """
        return pd.Timestamp(str(self.next()), tz=timezone.utc) - pd.Timedelta(1)


class RangeOfMonths(NamedTuple):
    start: MonthInYear
    stop: MonthInYear

    @classmethod
    def of(cls, timeseries: pd.Series) -> RangeOfMonths:
        return cls(
            MonthInYear.from_timestamp(timeseries.min()),
            MonthInYear.from_timestamp(timeseries.max()),
        )

    def as_slice(self) -> slice:
        """
        Convert the range-of-months into a slice with the range's starting and
        stopping months-in-years.
        """
        return slice(self.start, self.stop)


def time_range(begin: str, end: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Determine the first and last moments for the given month-in-years in YYYY-MM
    format.
    """
    return (
        MonthInYear.from_yyyy_mm(begin).start,
        MonthInYear.from_yyyy_mm(end).stop,
    )

from __future__ import annotations
from calendar import monthrange
from collections.abc import Iterator
from dataclasses import dataclass
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
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


@dataclass(frozen=True, kw_only=True, order=True)
class MonthInYear:
    """A specific month in a specific year."""

    year: int
    month: int

    @classmethod
    def from_mmm_yyyy(cls, text: str) -> MonthInYear:
        """
        Create month in year from first three letters of month, separator
        character, and four digit year.
        """
        assert len(text) == 8

        year = int(text[4:])
        month = _SHORT_MONTHS.index(text[:3]) + 1
        return cls(year=year, month=month)

    @staticmethod
    def is_yyyy_mm(text: str) -> bool:
        if len(text) != 7:
            return False
        return bool(_YYYY_MM.fullmatch(text))

    @classmethod
    def from_yyyy_mm(cls, text: str) -> MonthInYear:
        """
        Create month in year from four digit year, separator character, and two
        digit month.
        """
        assert len(text) == 7

        year = int(text[:4])
        month = int(text[-2:])
        return cls(year=year, month=month)

    @classmethod
    def from_timestamp(cls, timestamp: pd.Timestamp) -> MonthInYear:
        """Create month in year from Pandas timestamp."""
        return cls(year=timestamp.year, month=timestamp.month)

    def __len__(self) -> int:
        """Get number of days for this month in year."""
        _, days = monthrange(self.year, self.month)
        return days

    def __str__(self) -> str:
        return f"{self.year:04}-{self.month:02}"

    def __sub__(self, other: MonthInYear) -> int:
        return (self.year - other.year) * 12 + (self.month - other.month)

    def __iter__(self) -> Iterator[MonthInYear]:
        cursor = self
        while True:
            yield cursor
            cursor = cursor.next()

    def next(self) -> MonthInYear:
        year = self.year
        month = self.month + 1
        if month == 13:
            year += 1
            month = 1
        return self.__class__(year=year, month=month)

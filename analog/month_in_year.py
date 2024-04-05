from __future__ import annotations
from calendar import month_abbr, monthrange
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Literal, NamedTuple, overload, Protocol, runtime_checkable, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


SHORT_MONTHS = (
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
)


@runtime_checkable
class MonthInYearly(Protocol):
    """
    The month-in-year protocol, which requires`year` and `month` properties
    only. `MonthInYear` obviously implements the protocol. So do the standard
    library's `datetime.datetime` and Panda's `pandas.timestamp`.
    """

    @property
    def year(self) -> int:
        ...

    @property
    def month(self) -> int:
        ...


class MonthInYear(NamedTuple):
    """A specific month in a specific year."""

    year: int
    month: int

    @overload
    @classmethod
    def of(cls, value: MonthInYearly) -> MonthInYear:
        ...

    @overload
    @classmethod
    def of(cls, value: str) -> MonthInYear:
        ...

    @classmethod
    def of(cls, value: MonthInYearly | str) -> MonthInYear:
        """
        Convert the given value into a month-in-year. This method recognizes
        strings in mmm-yyyy format, using the first three letters of the month
        in the English language, or yyyy-mm format. It also coerces any
        month-in-yearly instance into a proper month-in-year.
        """

        if isinstance(value, MonthInYearly):
            return cls(value.year, value.month)

        assert isinstance(value, str), f'value "{value}" of type {type(value)}'

        if len(value) == 8:
            # mmm-yyy format
            try:
                return cls(int(value[4:]), SHORT_MONTHS.index(value[:3].casefold()) + 1)
            except:
                raise ValueError(f'malformed mmm-yyyy string "{value}"')

        if len(value) == 7:
            # yyyy-mm format
            try:
                return cls(int(value[:4]), int(value[-2:]))
            except:
                raise ValueError(f'malformed yyyy-mm string "{value}"')

        raise ValueError(f'invalid month-in-year string "{value}"')

    def days(self) -> int:
        """Get number of days for this month in year."""
        _, days = monthrange(self.year, self.month)
        return days

    def __str__(self) -> str:
        return f"{self.year:04}-{self.month:02}"

    @overload
    def __add__(self, other: int) -> MonthInYear:
        ...

    def __add__(self, other: object) -> MonthInYear:
        """Add the given number of months to this month-in-year."""
        if not isinstance(other, int):
            return NotImplemented

        year = self.year + other // 12
        month = self.month + other % 12
        if month > 12:
            year += 1
            month -= 12
        return type(self)(year, month)

    @overload
    def __sub__(self, other: int) -> MonthInYear:
        ...
    @overload
    def __sub__(self, other: MonthInYearly) -> int:
        ...

    def __sub__(self, other: object) -> int:
        if isinstance(other, int):
            year = self.year - other // 12
            month = self.month - other % 12
            if month < 1:
                year -= 1
                month += 12
            return type(self)(year, month)
        if isinstance(other, MonthInYearly):
            return (self.year - other.year) * 12 + (self.month - other.month)
        return NotImplemented

    def previous(self) -> MonthInYear:
        """Determine the previous month-in-year."""
        year = self.year
        month = self.month - 1
        if month == 0:
            year -= 1
            month = 12
        return type(self)(year, month)

    def next(self) -> MonthInYear:
        """Determine the next month-in-year."""
        year = self.year
        month = self.month + 1
        if month == 13:
            year += 1
            month = 1
        return type(self)(year, month)

    def start(self, tz: tzinfo = timezone.utc) -> datetime:
        """Determine the first moment for this month-in-year."""
        return datetime(self.year, self.month, 1, 0, 0, 0, 0, tz)

    def stop(self, tz: tzinfo = timezone.utc) -> datetime:
        """Determine the last moment for this month-in-year."""
        n = self.next()
        return datetime(n.year, n.month, 1, 0, 0, 0, 0, tz) - timedelta.resolution

    def start_of_period(
        self,
        months: Literal[2, 3, 4, 6] = 3,
        *,
        next: bool = False
    ) -> MonthInYear:
        """
        Determine the first month of the period containing this month-in-year
        (if `next` is `False`) or the period after the one containing this
        month-in-year (if `next` is `True`).
        """
        overage = (self.month - 1) % months
        if overage == 0:
            return self

        year = self.year
        month = self.month - overage + next * months
        if month > 12:
            year += 1
            month -= 12
        elif month < 1:
            year -= 1
            month += 12

        return type(self)(year, month)


def monthly_slice(series: pd.Series) -> slice:
    """
    Create a slice with the first and last months-in-year for the given time series.
    """
    return slice(MonthInYear.of(series.min()), MonthInYear.of(series.max()))


def monthly_period(
    start: str | MonthInYearly, stop: str | MonthInYearly, tz: tzinfo = timezone.utc
) -> tuple[datetime, datetime]:
    """
    Determine the first and last moments of the given months-in-year. The start
    month-in-year must either come before the stop month-in-year or be the same.
    """
    start_month = MonthInYear.of(start)
    stop_month = MonthInYear.of(stop)
    if start_month > stop_month:
        raise ValueError(f'{start_month} comes after {stop_month}')
    return start_month.start(tz), stop_month.stop(tz)


def quarterly_ticks(
    start: MonthInYear,
    stop: MonthInYear
) -> tuple[list[int], list[int]]:
    qstart = start.start_of_period(3, next=True)
    qstop = stop.start_of_period(3)

    labels = []
    cursor = qstart
    while cursor <= qstop:
        labels.append(month_abbr[cursor.month])
        cursor += 3

    # Add 1 to stop, so that the final quarter gets a position, too
    positions = [*range(qstart - start, qstop - start + 1, 3)]
    return positions, labels


def yearly_ticks(start: MonthInYear, stop: MonthInYear) -> tuple[list[int], list[int]]:
    ystart = MonthInYear(start.year if start.month == 1 else start.year + 1, 1)
    ystop = MonthInYear(stop.year if stop.month == 12 else stop.year - 1, 1)

    labels = []
    cursor = ystart
    while cursor <= ystop:
        labels.append(cursor.year)
        cursor += 12

    positions = [p + 0.5 for p in range(ystart - start + 4, ystop - start + 5, 12)]
    return positions, labels

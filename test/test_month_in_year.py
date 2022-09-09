from datetime import datetime, timedelta, timezone
from analog.month_in_year import MonthInYear, MonthInYearly
import pandas as pd


def test_month_in_year() -> None:
    t1 = pd.Timestamp("2020-04-01 01:02:03+00:00")
    assert isinstance(t1, MonthInYearly)

    dt1 = datetime(2020, 4, 1, 2, 34, 56, 0, timezone.utc)
    assert isinstance(dt1, MonthInYearly)

    m1 = MonthInYear.of("Dec-2020")

    assert isinstance(m1, MonthInYearly)
    assert m1.year == 2020
    assert m1.month == 12
    assert str(m1) == "2020-12"
    assert m1.days() == 31

    m1 = MonthInYear.of("2020-04")

    assert m1.year == 2020
    assert m1.month == 4
    assert str(m1) == "2020-04"
    assert m1.days() == 30

    # Iteration starts with the month in year.
    m2 = m1

    m2 = m2.previous()
    m2 = m2.previous()
    assert str(m2) == "2020-02"
    m2 = m2.previous()
    m2 = m2.previous()
    assert str(m2) == "2019-12"

    m2 = m2.next()
    m2 = m2.next()
    assert str(m2) == "2020-02"
    m2 = m2.next()
    m2 = m2.next()
    assert str(m2) == "2020-04"
    m2 = m2.next()
    m2 = m2.next()
    assert str(m2) == "2020-06"

    assert m1 == m1
    assert m1 <= m1
    assert m1 < m2
    assert m2 >= m2
    assert m2 > m1
    assert m1 != m2

    assert m1.start() == datetime(2020, 4, 1, 0, 0, 0, 0, timezone.utc)
    assert m1.stop() == datetime(2020, 4, 30, 23, 59, 59, 999999, timezone.utc)

    eastern = timezone(timedelta(hours=-5), "Eastern")
    start = m2.start(eastern)
    stop = m2.stop(eastern)

    assert start == datetime(2020, 6, 1, 0, 0, 0, 0, eastern)
    assert stop == datetime(2020, 6, 30, 23, 59, 59, 999999, eastern)

    assert start.astimezone(timezone.utc) == datetime(
        2020, 6, 1, 5, 0, 0, 0, timezone.utc
    )
    assert stop.astimezone(timezone.utc) == datetime(
        2020, 7, 1, 4, 59, 59, 999999, timezone.utc
    )

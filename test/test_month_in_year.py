from analog.month_in_year import MonthInYear, RangeOfMonths
import pandas as pd


def test_month_in_year() -> None:
    m1 = MonthInYear.from_mmm_yyyy("Dec-2020")

    assert m1.year == 2020
    assert m1.month == 12
    assert str(m1) == "2020-12"
    assert m1.days() == 31

    m1 = MonthInYear.from_yyyy_mm("2020-04")

    assert m1.year == 2020
    assert m1.month == 4
    assert str(m1) == "2020-04"
    assert m1.days() == 30

    assert not MonthInYear.is_yyyy_mm("May-2020")
    assert MonthInYear.is_yyyy_mm("2020-06")

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

    assert m1.start == pd.Timestamp("2020-04-01 00:00:00+00:00")
    assert m1.stop == pd.Timestamp("2020-04-30 23:59:59.999999999+00:00")
    assert m2.start == pd.Timestamp("2020-06-01 00:00:00+00:00")
    assert m2.stop == pd.Timestamp("2020-06-30 23:59:59.999999999+00:00")

    range = RangeOfMonths(m1, m2)
    assert range.as_slice() == slice((2020, 4), (2020, 6))
    assert range.as_slice() == slice(m1, m2)

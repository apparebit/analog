from analog.month_in_year import MonthInYear


def test_month_in_year() -> None:
    m1 = MonthInYear.from_mmm_yyyy("Mar-2020")

    assert m1.year == 2020
    assert m1.month == 3
    assert str(m1) == "2020-03"
    assert len(m1) == 31

    m1 = MonthInYear.from_yyyy_mm("2020-04")

    assert m1.year == 2020
    assert m1.month == 4
    assert str(m1) == "2020-04"
    assert len(m1) == 30

    assert not MonthInYear.is_yyyy_mm("May-2020")
    assert MonthInYear.is_yyyy_mm("2020-06")

    # Iteration starts with the month in year.
    it = iter(m1)

    m2 = next(it)
    assert str(m2) == "2020-04"

    m2 = next(it)
    assert str(m2) == "2020-05"

    m2 = m2.next()
    assert str(m2) == "2020-06"

    assert m1 == m1
    assert m1 <= m1
    assert m1 < m2
    assert m2 >= m2
    assert m2 > m1
    assert m1 != m2

import calendar

import matplotlib as mp

from .analyzer import Summary


def plot_requests_and_page_views(summary: Summary) -> object:
    # Plot the two curves
    ax = summary.data.plot()

    # Add thousands separators to the y-axis
    ax.get_yaxis().set_major_formatter(
        mp.ticker.FuncFormatter(lambda x, _: format(int(x), ","))
    )

    # Remove x-axis label
    ax.set_xlabel(None)

    # Month labels, once a quarter
    quarter_start = summary.start.start_of_period(3, next=True)
    quarter_stop = summary.stop.start_of_period(3)

    positions = []
    labels = []

    cursor = quarter_start
    while cursor <= quarter_stop:
        positions.append(cursor - summary.start)
        labels.append(calendar.month_abbr[cursor.month])
        cursor += 3

    ax.tick_params(axis="x", labelrotation=50, labelsize="small")
    ax.set_xticks(positions, labels=labels)

    # Minor ticks for all other months
    positions = []
    labels = []

    cursor = summary.start
    while cursor <= summary.stop:
        if (cursor.month - 1) % 3 != 0:
            positions.append(cursor - summary.start)
            labels.append("")
        cursor = cursor.next()

    ax.set_xticks(positions, labels=labels, minor=True)

    # Year labels, for every full year
    year_start = summary.start.start_of_period(12, next=True)
    year_stop = summary.stop.start_of_period(12)

    positions = []
    labels = []

    cursor = year_start
    while cursor <= year_stop:
        positions.append(cursor - summary.start + 6)
        labels.append(f"└──\u2009{cursor.year}\u2009──┘")
        cursor += 12

    sec = ax.secondary_xaxis(location=-0.1)
    sec.tick_params(axis="x", length=0, labelsize="small")
    sec.set_xticks(positions, labels=labels)
    sec.spines["bottom"].set_linewidth(0)

    return ax

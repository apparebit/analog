import calendar

import matplotlib as mp
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

from .analyzer import Summary


def plot_monthly_summary(summary: Summary) -> object:
    fig, ax = plt.subplots(figsize=[9, 6])

    data = summary.data
    xs = [n for n in range(1, len(data) + 1)]
    bottom = data["zeros"]
    width = 0.8
    ax.bar(x=xs, height=data["all_requests"], bottom=bottom, width=width, color="#9e9e9e")
    ax.bar(x=xs, height=data["successful"], bottom=bottom, width=width, color="#90bf61")
    ax.bar(x=xs, height=data["page_views"], bottom=bottom, width=width, color="#417301")

    bottom += data["successful"]
    ax.bar(x=xs, height=data["redirected"], bottom=bottom, width=width, color="#fdc445")

    bottom += data["redirected"]
    ax.bar(x=xs, height=data["client_errors"], bottom=bottom, width=width, color="#f78a84")

    bottom += data["client_errors"]
    ax.bar(x=xs, height=data["server_errors"], bottom=bottom, width=width, color="#b1272e")

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

    handles = [mpatches.Patch(color=c) for c in [
        "#9e9e9e", "#b1272e", "#f78a84", "#fdc445", "#90bf61", "#417301"
    ]]
    labels = [
        "Bots", "Server Errors", "Client Errors", "Redirects", "Successful Requests", "Page Views"
    ]
    ax.legend(handles, labels)
    ax.set_title("HTTP Traffic for apparebit.com")

    return ax

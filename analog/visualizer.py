import calendar
from typing import Any

import matplotlib as mp
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

from .analyzer import Summary


def add_x_axis(ax: Any, summary: Summary) -> None:
    # ------------------------------------- X-Axis -------------------------------------
    ax.set_xlabel(None)

    quarter_start = summary.start.start_of_period(3, next=True)
    quarter_stop = summary.stop.start_of_period(3)

    # Major ticks every 3 months
    positions = []
    labels = []

    cursor = quarter_start
    while cursor <= quarter_stop:
        positions.append(cursor - summary.start)
        labels.append(calendar.month_abbr[cursor.month])
        cursor += 3

    ax.tick_params(axis="x", labelrotation=50, labelsize="small")
    ax.set_xticks(positions, labels=labels)

    # Minor ticks for remaining months
    positions = []
    labels = []

    cursor = summary.start
    while cursor <= summary.stop:
        if (cursor.month - 1) % 3 != 0:
            positions.append(cursor - summary.start)
            labels.append("")
        cursor = cursor.next()

    ax.set_xticks(positions, labels=labels, minor=True)

    # Year labels for every full year
    year_start = summary.start.start_of_period(12, next=True)
    year_stop = summary.stop.start_of_period(12)

    positions = []
    labels = []

    cursor = year_start
    while cursor <= year_stop:
        positions.append(cursor - summary.start + 6)
        labels.append(f"└────\u200a{cursor.year}\u200a────┘")
        cursor += 12

    sec = ax.secondary_xaxis(location=-0.1)
    sec.tick_params(axis="x", length=0, labelsize="small")
    sec.set_xticks(positions, labels=labels)
    sec.spines["bottom"].set_linewidth(0)


# ======================================================================================


def plot_monthly_summary(summary: Summary) -> object:
    fig, ax = plt.subplots(figsize=[9, 6])

    # ----------------------------------- Render Bars ----------------------------------
    data = summary.data
    xs = [n for n in range(1, len(data) + 1)]
    bottom = data["zeros"]
    width = 0.8

    ax.bar(x=xs, height=data["all_requests"], bottom=bottom, width=width, color="#9e9e9e")
    ax.bar(x=xs, height=data["successful"], bottom=bottom, width=width, color="#90bf61")
    ax.bar(x=xs, height=data["page_views"], bottom=bottom, width=width, color="#417301")

    # Do NOT use += because it modifies the series in place
    bottom = bottom + data["successful"]
    ax.bar(x=xs, height=data["redirected"], bottom=bottom, width=width, color="#fdc445")

    bottom = bottom + data["redirected"]
    ax.bar(x=xs, height=data["client_errors"], bottom=bottom, width=width, color="#f78a84")

    bottom = bottom + data["client_errors"]
    ax.bar(x=xs, height=data["server_errors"], bottom=bottom, width=width, color="#b1272e")

    # ----------------------------------- Y & X Axis -----------------------------------
    ax.get_yaxis().set_major_formatter(
        mp.ticker.FuncFormatter(lambda x, _: format(int(x), ","))
    )

    add_x_axis(ax, summary)

    # --------------------------------- Legend & Title ---------------------------------
    handles = [mpatches.Patch(color=c) for c in [
        "#9e9e9e", "#b1272e", "#f78a84", "#fdc445", "#90bf61", "#417301"
    ]]
    labels = [
        "Bots",
        "5xx Server Errors",
        "4xx Client Errors",
        "3xx Redirects",
        "2xx Successful Requests",
        "Page Views"
    ]
    ax.legend(handles, labels)
    ax.set_title("HTTPS Traffic for apparebit.com")

    return ax


# ======================================================================================


def plot_monthly_percentage(summary: Summary) -> object:
    fig, ax = plt.subplots(figsize=[9, 6])

    # ----------------------------------- Render Bars ----------------------------------
    data = summary.data
    xs = [n for n in range(1, len(data) + 1)]
    bottom = data["zeros"]
    width = 0.8

    total = data["all_requests"]
    successful = data["successful"] / total * 100
    page_views = data["page_views"] / total * 100
    redirected = data["redirected"] / total * 100

    ax.bar(x=xs, height=successful, bottom=bottom, width=width, color="#90bf61")
    ax.bar(x=xs, height=page_views, bottom=bottom, width=width, color="#417301")

    # Do NOT use += because it modifies the series in place
    bottom = bottom + successful
    ax.bar(x=xs, height=redirected, bottom=bottom, width=width, color="#fdc445")

    # ----------------------------------- Y & X Axis -----------------------------------
    ax.set_ylim(0, 100)
    ax.get_yaxis().set_major_formatter(
        mp.ticker.FuncFormatter(lambda x, _: f"{int(x)}%")
    )
    ax.axhline(50, color="#666", ls=(0, (10, 3)), lw=0.5, zorder=-665)
    ax.text(22, 51, "50%", size="large", weight="medium", style="italic", color="#666")

    add_x_axis(ax, summary)

    # --------------------------------- Legend & Title ---------------------------------
    handles = [mpatches.Patch(color=c) for c in ["#fdc445", "#90bf61", "#417301"]]
    labels = [
        "3xx Redirects",
        "2xx Successful Requests",
        "Page Views"
    ]
    ax.legend(handles, labels)
    ax.set_title("Useful Traffic as Fraction of Total (apparebit.com)")

    return ax


# ======================================================================================


def plot_visitors(summary: Summary) -> object:
    fig, ax = plt.subplots(figsize=[9, 6])

    # ---------------------------------- Render Lines ----------------------------------
    data = summary.data
    xs = [n for n in range(1, len(data) + 1)]
    has_monthly = "monthly_visitors" in data

    line_colors = ["#f37ed3"] #"#9e2e84"]
    labels = ["Daily Unique Visitors"]
    ax.plot(xs, data["daily_visitors"], color="#f37ed3") #"#9e2e84")

    if has_monthly:
        line_colors.append("#6ab3ff")
        labels.append("Monthly Unique Visitors")
        ax.plot(xs, data["monthly_visitors"], color="#6ab3ff")

    # ----------------------------------- Y & X Axis -----------------------------------
    ax.get_yaxis().set_major_formatter(
        mp.ticker.FuncFormatter(lambda x, _: format(int(x), ","))
    )

    add_x_axis(ax, summary)

    # --------------------------------- Legend & Title ---------------------------------
    handles = [mlines.Line2D([], [], color=c) for c in line_colors]
    ax.legend(handles, labels)
    ax.set_title("Unique Visitors (apparebit.com)")

    return ax

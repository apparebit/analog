import sys

STEPS = 4
WIDTH = 25
COLUMN0 = "\x1b[0G"
GREEN = "\x1b[92m"
DEFAULT = "\x1b[39m"

def progress(percent: float, label: str = "") -> None:
    """
    Display a progress bar for the given percentage on the current terminal
    line.
    """
    percent = min(percent, 100)

    full, partial = divmod(round(percent), STEPS)
    empty = WIDTH - full - (1 if 0 < partial else 0)
    cells = (
        ("█" * full) +
        (["▎", "▌", "▊"][partial - 1] if 0 < partial else "") +
        (" " * empty)
    )

    sys.stdout.write(f"{COLUMN0}  ┫{GREEN}{cells}{DEFAULT}┣ {percent: 5.1f}% {label:<25}")
    sys.stdout.flush()

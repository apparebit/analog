import argparse
import os
from pathlib import Path
import sys

import konsole

from .error import StorageError


NUMEXPR_MAX_THREADS = "NUMEXPR_MAX_THREADS"


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("python -m analog")

    cwd = os.getcwd()
    parser.add_argument(
        "root",
        nargs="?",
        default=cwd,
        help=f"set the root directory with log data (default: {cwd})",
    )

    parser.add_argument(
        "--clean",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="remove combined dataframes and their sidecar files",
    )

    parser.add_argument(
        "--incr",
        dest="incremental",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="incrementally build combined dataframe (experimental)",
    )

    parser.add_argument(
        "--color",
        dest="use_color",
        default=None,
        action=argparse.BooleanOptionalAction,
        help="forcibly enable or disable color output",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=-1,
        dest="volume",
        help="increase verbosity",
    )

    return parser


def to_options(args: list[str]) -> argparse.Namespace:
    parser = create_parser()
    options = parser.parse_args(args)
    options.root = Path(options.root)

    konsole.config(use_color=options.use_color, volume=options.volume)
    konsole.info("Running with configuration", detail=vars(options))

    return options


def main(args: None | list[str] = None) -> None:
    # If the maximum number of threads for numexpr is not configured, use
    # minimum. This suppresses an annoying announcement to the log.
    if NUMEXPR_MAX_THREADS not in os.environ:
        os.environ[NUMEXPR_MAX_THREADS] = "8"

    try:
        options = to_options(sys.argv[1:] if args is None else args)

        # Delay import of data manager module until after numexpr has been configured.
        from .data_manager import latest_log_data

        frame, _ = latest_log_data(**vars(options))

        # Monthly page views: successful GET requests for markup not made by bots.
        from .analyzer import analyze

        page_views = (
            analyze(frame)
            .only.successful()
            .only.GET()
            .only.markup()
            .only.humans()
            .monthly.requests()
            .data
        )

        s = page_views.to_string()
        s = s.replace("-01 00:00:00+00:00", "")
        konsole.info("Monthly page views", detail=s.splitlines()[1:-1])

    except KeyboardInterrupt:
        konsole.warning("Analog detected keyboard interrupt, exiting...")
    except StorageError as x:
        konsole.critical(x.args[0])
    except Exception as x:
        konsole.critical(
            "Oh dear, something has gone terribly wrong: %s", x, exc_info=x
        )
    else:
        konsole.info("Happy, happy, joy, joy!")


if __name__ == "__main__":
    main()

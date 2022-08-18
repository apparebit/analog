import argparse
import os
from pathlib import Path
import sys

import konsole


NUMEXPR_MAX_THREADS = "NUMEXPR_MAX_THREADS"


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser("Analyze server access logs")

    parser.add_argument(
        "--root",
        help="Configure the root directory containing log data.",
    )

    parser.add_argument(
        "--clean",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Remove all files derrived from the access logs (but not hostnames).",
    )

    parser.add_argument(
        "--incremental",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reduce memory consumption by processing access logs piecemeal.",
    )

    parser.add_argument(
        "--use-color",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Toggle color output.",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        dest="volume",
        help="Increase verbosity",
    )

    return parser


def to_options(args: list[str]) -> argparse.Namespace:
    parser = create_parser()
    options = parser.parse_args(args)
    if options.root:
        options.root = Path(options.root)
    else:
        options.root = Path(__file__).parents[1] / "data"

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

        frame, _ = latest_log_data(options)

        from .analyzer import calculate

        series = (
            calculate(frame)
            .humans()
            .successfully()
            .getting()
            .markup()
            .per_month()
            .series
        )
        print(series.to_string())

    except KeyboardInterrupt:
        konsole.warning("Analog detected keyboard interrupt, exiting...")
    except Exception as x:
        konsole.critical("Oh dear, something has gone wrong: %s", x, exc_info=x)
    else:
        konsole.info("Happy, happy, joy, joy!")


if __name__ == "__main__":
    main()

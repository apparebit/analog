import argparse
import os
from pathlib import Path
import re
import sys

import konsole

from .error import AnalogError


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
        default=0,
        dest="volume",
        help="increase verbosity",
    )

    parser.add_argument(
        "--list-bots",
        action="store_true",
        help="list all bot names",
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

        # Delay import of main module until after numexpr has been configured.
        import analog

        frame = analog.latest(**vars(options))
        analog.validate(frame)

        if options.list_bots:
            list = frame[frame['is_bot1'] | frame['is_bot2']]['user_agent'].unique()
            list = sorted(list)
            for name in list:
                print(name)

        summary = analog.summarize(frame)
        ax = analog.plot_monthly_summary(summary)
        ax.figure.savefig(f"views-{summary.start}-{summary.stop}.svg", format="svg")

    except KeyboardInterrupt:
        konsole.warning("analog detected keyboard interrupt, exiting...")
    except AnalogError as x:
        konsole.critical(x.args[0])
    except Exception as x:
        konsole.critical(
            "oh dear, something has gone terribly wrong: %s", x, exc_info=x
        )
    else:
        konsole.info("happy, happy, joy, joy!")


if __name__ == "__main__":
    main()

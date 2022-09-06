from __future__ import annotations
from collections.abc import Iterator
import gzip
import json
from operator import itemgetter
from pathlib import Path
import shutil
from typing import Optional

import konsole
import pandas as pd
import pyarrow
import pyarrow.parquet

from .atomic_update import atomic_update
from .error import StorageError
from .month_in_year import MonthInYear
from .parser import enrich, parse_all_lines
from .schema import coerce, validate


__all__ = ('Coverage', 'DataManager', 'latest_log_data', 'validate_log_data')


class Coverage:
    """The log data's monthly coverage."""

    def __init__(self, enriched_log_path: Path) -> None:
        if not enriched_log_path.exists():
            raise StorageError(
                f'Directory "{enriched_log_path}" with enriched logs does not exist'
            )

        self._root_path = enriched_log_path.parent
        self._ingested_logs = sorted(
            (
                (MonthInYear.from_yyyy_mm(p.stem[-7:]), p)
                for p in enriched_log_path.glob("*-????-??.parquet")
            ),
            key=itemgetter(0),
        )

        if not self._ingested_logs:
            raise StorageError(
                f'Directory "{enriched_log_path}" exits but contains no enriched logs'
            )

        self._domain: str = self._ingested_logs[0][1].stem[:-8]

        if any(self._domain != p.stem[:-8] for _, p in self._ingested_logs):
            raise StorageError(
                f'Directory "{enriched_log_path}" contains enriched logs '
                'for several domains'
            )

        self._begin: MonthInYear = self._ingested_logs[0][0]
        self._end: MonthInYear = self._ingested_logs[-1][0]

        self._days = 0
        self._months = 0

        self._monthly_requests: dict[str, dict[str, int]] = {}
        self._total_requests = 0

    @property
    def begin(self) -> MonthInYear:
        return self._begin

    @property
    def end(self) -> MonthInYear:
        return self._end

    def name_with_suffix(self, suffix: str) -> str:
        return f"{self._domain}-{self._begin}-{self._end}{suffix}"

    def path_with_suffix(self, suffix: str) -> Path:
        return self._root_path / self.name_with_suffix(suffix)

    def ingested_logs(self) -> Iterator[tuple[MonthInYear, Path]]:
        return iter(self._ingested_logs)

    def register_log_data(self, month_in_year: MonthInYear, data: pd.DataFrame) -> None:
        key = str(month_in_year)

        assert self._begin <= month_in_year <= self._end
        assert key not in self._monthly_requests

        self._days += month_in_year.days()
        self._months += 1

        requests = len(data)
        self._monthly_requests[key] = {"requests": requests}
        self._total_requests += requests

    def summary(self) -> dict[str, object]:
        cursor = self._begin
        days = cursor.days()
        months = 1
        missing: list[str] = []

        while cursor < self._end:
            cursor = cursor.next()
            key = str(cursor)
            if key in self._monthly_requests:
                days += cursor.days()
                months += 1
            else:
                missing.append(key)

        assert days == self._days
        assert months == self._months
        assert months == len(self._monthly_requests)

        return {
            "domain": self._domain,
            "requests": self._total_requests,
            "days": self._days,
            "months": self._months,
            "begin": str(self._begin),
            "end": str(self._end),
            "missing": ", ".join(missing) if missing else None,
        }

    def save(self) -> None:
        coverage = self._monthly_requests | {"summary": self.summary()}
        with atomic_update(self.path_with_suffix(".json")) as file:
            json.dump(coverage, file, indent=4, sort_keys=True)


# --------------------------------------------------------------------------------------


class DataManager:
    """A data manager."""

    @staticmethod
    def _check_directory_exists(path: Path, is_root: bool) -> None:
        if path.is_dir():
            return
        if is_root:
            raise StorageError(f'Data directory "{path}" does not exist')
        raise StorageError(
            f'Data directory "{path.parent}" ' f'has no "{path.name}" subdirectory'
        )

    @staticmethod
    def _find_latest_location_db(path: Path) -> Path:
        databases = [p for p in path.glob("city-????-??-??.mmdb") if p.is_file()]
        if databases:
            return max(databases)
        raise StorageError(
            f'"{path}" contains no IP location database '
            'named like "city-2022-07-11.mmdb"'
        )

    def __init__(self, root: Path) -> None:
        # Set up this data manager's configuration state.
        self._root = root
        DataManager._check_directory_exists(root, is_root=True)

        self._access_log_path = root / "access-logs"
        DataManager._check_directory_exists(self._access_log_path, is_root=False)
        self._enriched_log_path = root / "enriched-logs"
        self._enriched_log_path.mkdir(exist_ok=True)

        self._hostname_db_path = root / "hostnames.json"
        location_database_path = root / "location-db"
        DataManager._check_directory_exists(location_database_path, is_root=False)
        self._location_db_path = DataManager._find_latest_location_db(
            location_database_path
        )

        # Set up access log state.
        self._domain: Optional[str] = None
        self._did_ingest_access_log: Optional[bool] = None

        self._log_data_path: Optional[Path] = None
        self._log_data: Optional[pd.DataFrame] = None
        self._coverage: Optional[Coverage] = None

    @property
    def data(self) -> pd.DataFrame:
        assert self._log_data is not None
        return self._log_data

    @property
    def coverage(self) -> Coverage:
        assert self._coverage is not None
        return self._coverage

    def clean_monthly_logs(self) -> None:
        """Delete previously ingested monthly logs."""
        # Delete all enriched logs but restore empty directory.
        shutil.rmtree(self._enriched_log_path, ignore_errors=True)
        self._enriched_log_path.mkdir()

    def _parse_access_log_name(self, path: Path) -> MonthInYear:
        """Parse name of log file into domain and month of year."""
        month_in_year = MonthInYear.from_mmm_yyyy(path.stem[-8:])
        domain = path.stem[:-17]
        if self._domain is None:
            self._domain = domain
        if self._domain == domain:
            return month_in_year

        raise StorageError(
            f'"{self._access_log_path}" contains access logs for '
            f'domains {self._domain} and {domain}'
        )

    def parse_and_enrich_log(self, path: Path) -> pd.DataFrame:
        """
        Parse and enrich the access log at the given path, convert the result to
        a dataframe and return it.
        """
        with gzip.open(path, mode="rt", encoding="utf8") as lines:
            log_data = parse_all_lines(lines)

        enrich(log_data, self._hostname_db_path, self._location_db_path)
        return coerce(pd.DataFrame(log_data))

    def ingest_monthly_logs(self) -> None:
        """
        Ingest all access logs by parsing and enriching monthly log files. This method
        skips a monthly log if a Parquet file with the enriched data already exists.
        """
        # Process access logs in chronological order.
        source_paths = sorted(
            self._access_log_path.glob("*-ssl_log-???-????.gz"),
            key=lambda p: MonthInYear.from_mmm_yyyy(p.stem[-8:]),
        )

        did_ingest_access_log = False
        for source_path in source_paths:
            if not source_path.is_file():
                continue

            month_in_year = self._parse_access_log_name(source_path)
            target_path = (
                self._enriched_log_path / f"{self._domain}-{month_in_year}.parquet"
            )
            if target_path.exists():
                continue

            konsole.info(
                "Ingest request data for %s",
                month_in_year,
                detail={"from": source_path, "to": target_path},
            )

            df = self.parse_and_enrich_log(source_path)
            df.to_parquet(target_path)
            did_ingest_access_log = True

        self._did_ingest_access_log = did_ingest_access_log

    def _write_incrementally(self, coverage: Coverage, target_path: Path) -> None:
        # Table.from_pandas() doesn't just translate from Pandas' column
        # types to Parquet's. It adds further metadata that helps convert
        # back to the same Pandas datatypes again. Hence we don't provide
        # our own schema but delegate to Table.from_pandas(). Alas, the
        # resulting Parquet file has as many "row groups" as data frames
        # written below and is ~20% larger than a Parquet file created by
        # writing a single data frame. Hence we avoid this approach by
        # default.

        def read_table(month_in_year: MonthInYear, path: Path) -> pyarrow.Table:
            frame = pd.read_parquet(path)
            coverage.register_log_data(month_in_year, frame)
            return pyarrow.Table.from_pandas(frame)

        it = coverage.ingested_logs()
        table = read_table(*next(it))
        with pyarrow.parquet.ParquetWriter(target_path, table.schema) as parquet:
            parquet.write_table(table)

            for month_in_year, source_path in it:
                parquet.write_table(read_table(month_in_year, source_path))

        coverage.save()
        self._log_data = pd.read_parquet(target_path)

    def _combine_and_write(self, coverage: Coverage, target_path: Path) -> None:
        frames = []
        for month_in_year, source_path in coverage.ingested_logs():
            frame = pd.read_parquet(source_path)
            coverage.register_log_data(month_in_year, frame)
            frames.append(frame)

        self._log_data = pd.concat(frames, ignore_index=True)
        self._log_data.to_parquet(target_path)
        coverage.save()

    def combine_monthly_logs(self, /, incremental: bool = False) -> None:
        """Combine monthly logs into a single data frame."""
        if self._did_ingest_access_log is None:
            raise ValueError("Please ingest access logs before combining them")

        self._coverage = Coverage(self._enriched_log_path)
        if self._did_ingest_access_log:
            for path in self._root.glob(self._coverage.name_with_suffix(".json")):
                path.unlink()
            for path in self._root.glob(self._coverage.name_with_suffix(".parquet")):
                path.unlink()
            self._did_ingest_access_log = False

        self._log_data_path = target_path = self._coverage.path_with_suffix(".parquet")
        if target_path.exists():
            konsole.info("Load existing log frame '%s'", target_path)
            self._log_data = pd.read_parquet(target_path)

        elif incremental:
            konsole.info("Incrementally save log frame '%s'", target_path)
            self._write_incrementally(self._coverage, target_path)

        else:
            konsole.info("Build and save log frame '%s'", target_path)
            self._combine_and_write(self._coverage, target_path)


def latest_log_data(
    root: str | Path,
    clean: bool = False,
    incremental: bool = False,
    **_: object,
) -> pd.DataFrame:
    manager = DataManager(Path(root))
    if clean:
        manager.clean_monthly_logs()
    manager.ingest_monthly_logs()
    manager.combine_monthly_logs(incremental=incremental)
    return manager.data


validate_log_data = validate

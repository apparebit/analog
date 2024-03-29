from collections.abc import Sequence
from pathlib import Path

from geoip2.database import Reader as LocationDatabaseReader
from geoip2.errors import AddressNotFoundError
from geoip2.models import City as LocationData

import pandas as pd

from .error import StorageError


def latest_location_db_path(path: None | Path = None) -> Path:
    if path is None:
        path = Path('.') / 'data' / 'location-db'

    databases = [p for p in path.glob("city-????-??-??.mmdb") if p.is_file()]
    if databases:
        return max(databases)
    raise StorageError(
        f'"{path}" contains no IP location database named like "city-2022-07-11.mmdb"')


def latest_location_db(path: None | Path = None) -> LocationDatabaseReader:
    return LocationDatabaseReader(latest_location_db_path(path))


def lookup_location(
    addr: str,
    db_reader: LocationDatabaseReader
) -> None | LocationData:
    try:
        return db_reader.city(addr)
    except AddressNotFoundError:
        return None


def country_db(path: None | Path = None) -> pd.DataFrame:
    if path is None:
        path = Path('.') / 'data' / 'countries.csv'
    with open(path, mode='rt', encoding='utf8') as fs:
        return pd.read_csv(path, header=0, index_col=False)


def lookup_country(iso2: str, countries: pd.DataFrame) -> None | str:
    names = countries[countries['iso2'] == iso2]['country']
    count = len(names)
    if count == 1:
        return names.iloc[0]
    else:
        return None


def resolve_to_countries(addrs: Sequence[str]) -> list[tuple[str, None | str]]:
    countries = country_db()
    records: list[tuple[str, None | str]] = []

    with latest_location_db() as reader:
        for addr in addrs:
            record = lookup_location(addr, reader)
            if record is None or record.country.iso_code is None:
                records.append((addr, None))
                continue
            iso2 = record.country.iso_code
            country = lookup_country(iso2, countries)
            records.append((addr, country))

    return records

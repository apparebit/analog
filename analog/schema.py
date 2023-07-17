from collections.abc import Sized
from types import MappingProxyType
from typing import NamedTuple

import numpy as np
import pandas as pd
from pandas.core.dtypes.base import ExtensionDtype

from .label import (
    ContentType,
    HttpMethod,
    HttpProtocol,
    HttpScheme,
    HttpStatus,
)


BoolDtype = np.dtype(bool)
Float64Dtype = np.dtype(float)
Int16Dtype = np.dtype('int16')
Int32Dtype = np.dtype('int32')
StringDtype = pd.StringDtype()


ACCESS_LOG_COLUMNS: MappingProxyType[str, np.dtype | ExtensionDtype] = MappingProxyType(
    {
        "client_address": StringDtype,
        "timestamp": pd.DatetimeTZDtype('ns', 'UTC'),
        "method": pd.CategoricalDtype(categories=list(HttpMethod)),
        "path": StringDtype,
        "query": StringDtype,
        "fragment": StringDtype,
        "protocol": pd.CategoricalDtype(categories=list(HttpProtocol), ordered=True),
        "status": Int16Dtype,
        "size": Int32Dtype,
        "referrer": StringDtype,
        "user_agent": StringDtype,
        "server_name": StringDtype,
        "server_address": StringDtype,
    }
)

DERIVED_COLUMNS: MappingProxyType[str, np.dtype | ExtensionDtype] = MappingProxyType(
    {
        "cool_path": StringDtype,
        "content_type": pd.CategoricalDtype(categories=list(ContentType)),
        "status_class": pd.CategoricalDtype(categories=list(HttpStatus), ordered=True),
        "referrer_scheme": pd.CategoricalDtype(categories=list(HttpScheme)),
        "referrer_host": StringDtype,
        "referrer_path": StringDtype,
        "referrer_query": StringDtype,
        "referrer_fragment": StringDtype,
    }
)

ENRICHED_COLUMNS: MappingProxyType[str, np.dtype | ExtensionDtype] = MappingProxyType(
    {
        # DB: DNS
        "client_name": StringDtype,
        # DB: GeoIP2
        "client_latitude": Float64Dtype,
        "client_longitude": Float64Dtype,
        "client_city": StringDtype,
        "client_country": StringDtype,
        # DB: https://github.com/robd003/uap-python-up2date
        "is_bot1": BoolDtype,
        # DB: https://github.com/matomo-org/device-detector
        "is_bot2": BoolDtype,
    }
)

SCHEMA = MappingProxyType(ACCESS_LOG_COLUMNS | DERIVED_COLUMNS | ENRICHED_COLUMNS)

COLUMN_NAMES = frozenset(SCHEMA.keys())
NON_NULL_COLUMN_NAMES = frozenset(
    (
        'client_address',
        'timestamp',
        'method',
        'path',
        'protocol',
        'status',
        'size',
        'cool_path',
        'content_type',
        'status_class',
        'is_bot1',
        'is_bot2',
    )
)


class NullConstraint(NamedTuple):
    # Rows must be all null or non-null.
    equal: tuple[str, ...]
    # At least the null rows of equal columns must be null.
    at_least: tuple[str, ...]

    @classmethod
    def of(
        cls, *equal: str, at_least: tuple[str, ...] | None = None
    ) -> 'NullConstraint':
        return cls(tuple(equal), () if at_least is None else at_least)

    def apply_to(self, df: pd.DataFrame) -> None:
        """
        If this constraint does not hold for the given dataframe, signal a
        validation error.
        """
        column1 = self.equal[0]
        values1 = df[column1].notnull()

        for column2 in self.equal[1:]:
            values2 = df[column2].notnull()

            if values2.ne(values1).any():
                raise AttributeError(
                    f'{column1} and {column2} mix null and non-null values in same row'
                )

        for column2 in self.at_least:
            values2 = df[column2].notnull()

            if values2.__or__(values1).ne(values1).any():
                raise AttributeError(
                    f'{column2} has non-null values in rows that are null in {column1}'
                )


NULL_CONSTRAINTS = (
    # Web server logged virtual host
    NullConstraint.of('server_name', 'server_address'),
    # Analog parsed 'referer' header
    NullConstraint.of(
        'referrer_scheme',
        'referrer_host',
        at_least=('referrer_path', 'referrer_query', 'referrer_fragment'),
    ),
    # Location database resolved IP address
    NullConstraint.of('client_latitude', 'client_longitude'),
    # ua-parser resolved 'user-agent' header
    NullConstraint.of(
        'agent_family',
        'agent_version',
        'os_family',
        'os_version',
        'device_family',
        'device_brand',
        'device_model',
    ),
)


def coerce(data: pd.DataFrame) -> pd.DataFrame:
    """Coerce the given log dataframe to the log schema."""
    return data.astype(SCHEMA)


def validate(df: pd.DataFrame) -> None:
    """
    Validate the given dataframe. This function checks that the dataframe has
    the expected columns with the expected types, non-null columns do not
    contain null values, and columns with correlated null values in fact observe
    those correlations.
    """

    def plural(s: Sized) -> str:
        return 's' if len(s) != 1 else ''

    # ----------------------------------------------------------------------------------
    # No extra or missing columns

    columns = frozenset(df.columns)
    if not columns < COLUMN_NAMES:
        missing = columns - COLUMN_NAMES
        raise AttributeError(
            f'dataframe is missing column{plural(missing)} {", ".join(missing)}'
        )

    # ----------------------------------------------------------------------------------
    # Columns have expected types

    maltyped = [
        f'{column} has type {type(df.dtypes[column])} instead of {type(dtype)}'
        for column, dtype in SCHEMA.items()
        if df.dtypes[column] != dtype
    ]

    if maltyped:
        raise AttributeError('; '.join(maltyped))

    # ----------------------------------------------------------------------------------
    # Non-null columns contain no unexpected nulls

    for column in NON_NULL_COLUMN_NAMES:
        nulls = df[column].isna().sum()
        if nulls != 0:
            raise AttributeError(
                f'column "{column}" unexpectedly contains '
                f'{nulls} null{"s" if nulls != 1 else ""}'
            )

    # ----------------------------------------------------------------------------------
    # Constraints on non-null values hold

    for constraint in NULL_CONSTRAINTS:
        constraint.apply_to(df)

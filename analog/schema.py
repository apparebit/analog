from collections.abc import Mapping, Sized
from types import MappingProxyType

from .error import ValidationError
from .label import ContentType, HttpMethod, HttpProtocol, HttpScheme, HttpStatus

import numpy as np
import pandas as pd

# The order of fields in the log schema below is the same as the order of fields
# in the dictionaries returned from both parser:parse_line() and
# parser:parse_all_lines() plus the fields added by parser:enrich():
#
#  1. client_address to server_address are capturing groups of _LINE_PATTERN regex.
#  2. content_type to status_class are added by parse_line() to simplify analysis;
#  3. client_name to is_bot are added by enrich() to add further information.

SCHEMA: Mapping[str, str | pd.CategoricalDtype] = MappingProxyType(
    {
        # Properties included in access log
        "client_address": "string",
        "timestamp": "datetime64[ns, UTC]",
        "method": pd.CategoricalDtype(categories=tuple(HttpMethod)),
        "path": "string",
        "query": "string",
        "fragment": "string",
        "protocol": pd.CategoricalDtype(categories=tuple(HttpProtocol), ordered=True),
        "status": "int16",
        "size": "int32",
        "referrer": "string",
        "user_agent": "string",
        "server_name": "string",
        "server_address": "string",
        # Properties added after the fact
        "content_type": pd.CategoricalDtype(categories=tuple(ContentType)),
        "cool_path": "string",
        "referrer_scheme": pd.CategoricalDtype(categories=tuple(HttpScheme)),
        "referrer_host": "string",
        "referrer_path": "string",
        "referrer_query": "string",
        "referrer_fragment": "string",
        "status_class": pd.CategoricalDtype(categories=tuple(HttpStatus), ordered=True),
        # Enriched properties
        "client_name": "string",
        "client_latitude": "float64",
        "client_longitude": "float64",
        "client_city": "string",
        "client_country": "string",
        "agent_family": "string",
        "agent_version": "string",
        "os_family": "string",
        "os_version": "string",
        "device_family": "string",
        "device_brand": "string",
        "device_model": "string",
        "is_bot": "bool",
    }
)


def coerce(data: pd.DataFrame) -> pd.DataFrame:
    """Coerce the given log dataframe to the log schema."""
    return data.astype(SCHEMA, copy=False)  # type: ignore


def validate(df: pd.DataFrame) -> None:
    """Validate that the given log dataframe has the log schema."""

    def plural(s: Sized) -> str:
        return 's' if len(s) != 1 else ''

    # -------------------------- No extra or missing columns ---------------------------
    actual_columns = set(df.columns)
    expected_columns = set(SCHEMA.keys())

    if actual_columns != expected_columns:
        missing = expected_columns - actual_columns
        if missing:
            msg = f'dataframe is missing column{plural(missing)} {", ".join(missing)}'
        else:
            msg = ''

        extras = actual_columns - expected_columns
        if extras:
            msg = f'{msg}, ' if msg else 'dataframe '
            msg += f'has extra column{plural(extras)} {", ".join(extras)}'

        raise ValidationError(msg)

    # -------------------------- No columns with object type ---------------------------
    object_dt = np.dtype(object)
    maltyped = [c for c in SCHEMA.keys() if df.dtypes[c] == object_dt]
    if maltyped:
        msg = 'dataframe has object-typed '
        msg += f'column{plural(maltyped)} {", ".join(maltyped)}'
        raise ValidationError(msg)

    # ----------------------- Categorical columns are just that ------------------------
    maltyped = [
        name
        for name, dtype in SCHEMA.items()
        if isinstance(dtype, pd.CategoricalDtype) and df.dtypes[name] != dtype
    ]

    if maltyped:
        msg = 'dataframe has non-categorical '
        msg += f'column{plural(maltyped)} {", ".join(maltyped)}'
        raise ValidationError(msg)

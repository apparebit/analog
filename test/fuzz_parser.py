import atheris

# Analog's log parser makes heavy use of regular expressions.
# atheris.enabled_hooks.add("RegEx")

with atheris.instrument_imports():
    from datetime import datetime, timedelta, timezone

    # import re
    from analog.error import ParseError
    from analog.month_in_year import SHORT_MONTHS
    from analog.parser import parse_line

import json
import sys


EASTERN = timezone(timedelta(hours=-5), 'Eastern')


@atheris.instrument_func
def test_one_input(input):
    fdp = atheris.FuzzedDataProvider(input)
    value = fdp.ConsumeInt(4)

    line = f'{value & 0xff}.{(value >> 8) & 0xff}.'
    line += f'{(value >> 16) & 0xff}.{(value >> 24) & 0xff}'
    line += ' - - '

    timestamp = datetime.fromtimestamp(
        fdp.ConsumeFloatInRange(
            datetime(2002, 9, 1, 9, 0, 0, 0, EASTERN).timestamp(),
            datetime(2022, 9, 6, 11, 12, 35, 0, EASTERN).timestamp(),
        ),
        EASTERN,
    )

    month = SHORT_MONTHS[timestamp.month - 1].capitalize()
    line += f'[{timestamp.day}/{month}/{timestamp.year}'
    line += f':{timestamp.hour:02}:{timestamp.minute:02}:{timestamp.second:02} -0500]'

    # fmt: off
    STATUS_CODES = [
        200, 204, 206,
        307, 308,
        400, 401, 403, 404, 408, 409, 410, 411, 412, 413, 414, 418, 429,
        500, 501, 503,
    ]
    # fmt: on

    path = fdp.ConsumeUnicode(fdp.ConsumeIntInRange(0, 87))
    line += f'"GET {path} HTTP/2.0" '
    line += f'{fdp.PickValueInList(STATUS_CODES)} {fdp.ConsumeIntInRange(0, 419024)} '

    referrer = fdp.ConsumeUnicode(fdp.ConsumeIntInRange(0, 82))
    if len(referrer) < len('http://cnn.com'):
        referrer = '-'
    else:
        referrer = f'"{referrer}"'
    ua = fdp.ConsumeUnicode(fdp.ConsumeIntInRange(0, 200))
    line += f'{json.dumps(referrer)} "{json.dumps(ua)}"'

    props = parse_line(line)
    assert props is None or isinstance(props, dict)


atheris.Setup(sys.argv, test_one_input)
atheris.Fuzz()

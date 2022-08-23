# Ana(lyze) Log(s)

This package implements a modern, 2020s approach to analyzing webserver access
logs. It builds on two technologies that have become ubiquitous when it comes to
data processing:

  * [Notebooks](https://jupyter.org), which provide an effective graphical
    read-eval-print-loop (REPL);
  * [Pandas](https://pandas.pydata.org), which handles the low-level aspects
    of data wrangling with its *dataframe* abstraction.

Analog then adds:

  * Parsing and enriching the raw, textual access logs;
  * File management to automatically ingest monthly logs and combine them
    into a single dataframe;
  * A convenient, fluent interface that makes common analysis tasks easy,
    while seamlessly falling back onto Pandas for more complex tasks.


## Motivation

Many websites have switched to client analytics as a service. While certainly
convenient and often free, these services also have a terrible track record when
it comes to privacy and hence are entirely exploitative of website visitors.
Even when they are self-hosted, the necessary client code adds unnecessary bloat
to webpages. It also is far from guaranteed to produce meaningful results
because, by the time the code might run, users have already moved on or because
they have blocked the client code or JavaScript altogether.

Being more respectful of website visitors and hence removing invasive client
analytics is easy enough. But we'd still like to have *some* insight into how
visitors use our websites. Well, there are server access logs! Alas, in most
enterprises, those logs feed into larger log analytics and monitoring solutions,
which are overkill for an individual or small business using shared hosting.
Then there are the ancient [AWStats](https://awstats.sourceforge.io) and
[Webalizer](https://webalizer.net), typically included with the equally ancient
cPanel. Finally, there is the actively maintained
[GoAccess](https://goaccess.io). While pretty nifty, even that tool shows its
age: It's written in C and not exactly designed for extensibility or answering
ad-hoc queries.


## Analog

Analog relies on [notebooks](https://jupyter.org) for graphical REPL and
[Pandas](https://pandas.pydata.org) for low-level data wrangling. It then
adds a convenient, fluent interface that makes common analysis tasks easy.
It also manages monthly log files, parsing and enriching the raw access
logs as needed and automatically combining them into a single dataframe.


### Storage Management

Analog stores all data for a website in a dedicated directory. It uses three
subdirectories:

  * `access-logs` stores monthly access logs in files named like
    `apparebit.com-Aug-2022.gz`.

  * `enriched-logs` stores parsed and enriched monthly logs as
    [Parquet](https://parquet.apache.org) files named like
    `apparebit.com-2022-08.parquet`.

  * `location-db` stores IP location databases in GeoLite2 format named like
    `city-2022-07-26.mmdb`. Analog uses the most recent one.

Analog creates three files in its data directory:

  * The combined dataframe, again in Parquet format, is named like
    `apparebit.com-2018-07-2022-07.parquet`.

  * The metadata sidecar file in JSON format has the same name but with `.json`
    file extension.

  * `hostnames.json` caches previous DNS lookups of IP addresses, which are by
    far the slowest part of ingesting raw access logs.

When running analog from the command line or invoking the `latest_log_data()`
function in the `analog.data_manager` module, analog first ingests raw monthly
logs that have no corresponding enriched log files. Then, if there is no
combined log covering all monthly log files or one of those files was just
updated, analog creates a new combined log and its metadata sidecar file.

When running analog with the `--clean` command line option or setting the
`clean` argument to `latest_log_data()` to `True`, analog deletes all combined
logs and their sidecar files from the data directory. While analog does not
delete `enriched-logs` or its contents, you can safely do so as well because, as
just described, analog can always recreate enriched and combined log file. But
please, do not delete the `access-logs` or `hostnames.json`!


### Log Data Schema

Analog combines original properties extracted from the raw access log, derived
properties added while parsing, and enriching properties added after parsing
based on external databases for domain names, IP locations, and user agents. The
Python dictionary below shows [`analog.data_manager`'s
schema](https://github.com/apparebit/analog/blob/master/analog/data_manager.py#L32)
for log data. It makes use of [`analog.label`'s
enumerations](https://github.com/apparebit/analog/blob/master/analog/label.py)
`ContentType`, `HttpMethod`, `HttpProtocol`, `HttpScheme`, and `HttpStatus`.

```python
{
    # Original properties extracted from raw access log:
    "client_address": "string",
    "timestamp": "datetime64[ns, UTC]",
    "method": pd.CategoricalDtype(categories=tuple(HttpMethod)),
    "path": "string",
    "query": "string",
    "protocol": pd.CategoricalDtype(categories=tuple(HttpProtocol), ordered=True),
    "status": "int16",
    "size": "int32",
    "referrer": "string",
    "user_agent": "string",
    "server_name": "string",
    "server_address": "string",

    # Derived properties easily added during parsing:
    "content_type": pd.CategoricalDtype(categories=tuple(ContentType)),
    "cool_path": "string",
    "referrer_scheme": pd.CategoricalDtype(categories=tuple(HttpScheme)),
    "referrer_host": "string",
    "referrer_path": "string",
    "status_class": pd.CategoricalDtype(categories=tuple(HttpStatus), ordered=True),

    # Enriching properties that depend on external databases:
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
```


### Fluent Analysis

Analog's fluent interface makes use of computed properties as well as methods.
Properties typically distinguish between different types of clauses whereas
methods terminate the clauses. In the grammar below, property and method names
are double quoted. The attribute selector's period is written as `<dot>` and
methods are followed by `()`. A few methods have parameters written as
`<parameter-name>`.

The following grammar summarizes the fluent interface. At the top-level, a
**sentence** consists of phrases to specify (1) filters, (2) grouping and
selection, and (3) display:

    sentence 🠲 filters grouping-and-selection display

There are zero or more **filters**, each starting with the `.only` property and
followed by a method invocation:

    filters 🠲 filter filters | 𝜀

    filter 🠲 <dot> "only" <dot> filter-criterion()

    filter-criterion 🠲
        | "bots"
        | "humans"
        | "get"
        | "post"
        | "markup"
        | "successful"
        | "redirection"
        | "client_error"
        | "server_error"
        | "not_found"
        | "having" <enum-constant>

The `having()` method can filter on the `content_type`, `method`, `protocol` and
`status_class` column, which is implicit in the type of the given enumeration
constant. In fact, all other filter methods with the exception of `bots()`,
`humans()`, and `not_found()` are convenient aliases for invoking `having()`
with a specific enumeration constant.

**Grouping and selection** consists of either a rate or range followed by a
statistic. Range's `lifetime()` method covers the entire log and hence serves as
an effective bypass of rate or range. The `as_is` property serves as bypass of
grouping and selection. Requiring explicit bypass arguably is less elegant than
just omitting unnecessary clauses. At the same time, explicit bypass keeps the
implementation notably simpler.

    grouping-and-selection 🠲
        | rate statistic
        | range statistic
        | <dot> "as_is"

A **rate** is indicated by the `.monthly` property.

    rate 🠲 <dot> "monthly"

A **range** is indicated by the `.over` property followed by a method specifying
the date, time range. The `lifetime()` method covers the entire log. The
`last_day()`, `last_month()`, and `last_year()` methods arrive at their ranges
by computing the maximum timestamp for the wrapped log and then subtracting [a
suitable
offset](https://pandas.pydata.org/docs/user_guide/timeseries.html?highlight=dateoffset#dateoffset-objects).

    period 🠲 <dot> "over" <dot> date-range()

    date-range 🠲
        | "lifetime"
        | "last_day"
        | "last_month"
        | "last_year"
        | "range" <begin> <end>

Currently supported **statistics** are (1) the number of requests and (2) the
value counts for a given column. The `status_classes` and `content_types`
methods are convenient aliases for specific value counts.

    statistic 🠲 <dot> concrete-statistic()

    concrete-statistic 🠲
        | "requests"
        | "content_types"
        | "status_classes"
        | "value_counts" <column>

Finally, the optional **display** prints or plots the data — or does both.

    display 🠲 <dot> concrete-display() display | 𝜀

    concrete-display 🠲
        | "then_print"
        | "then_print" <row-count>
        | "then_plot" ```

The implementation generally follows the grammar. A class implementing a clause
typically has the same name as the corresponding nonterminal, though the name is
CamelCased and prefixed with `Fluent`. All classes representing nonterminals
inherit from the same base class `FluentTerm`, which holds the wrapped state and
provides convenient methods for creating new subclass instances. Since some
statistics result in series instead of dataframes, that base class and
`FluentDisplay` are generic.

The main entry point is:

    def analyze(frame: pd.DataFrame, cover: Coverage) -> FluentSentence: ...

A second function recombines several (wrapped) series into a dataframe, notably
for plotting:

    def merge(columns: dict[str, FluentTerm[pd.Series]]) -> FluentSentence: ...

Et voilà! 😎

---

© 2022 [Robert Grimm](https://apparebit.com).
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.
[GitHub](https://github.com/apparebit/analog).

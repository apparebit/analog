# Ana(lyze) Log(s)

A modern approach to analyzing webserver access logs!

  * Keep on reading for a detailed, top-down description of analog's features.
  * Peruse [this
    notebook](https://github.com/apparebit/analog/blob/master/docs/hands-on.ipynb)
    for a hands-on introduction using [my website's](https://apparebit.com) logs
    as example.
  * Consult [this
    grammar](https://github.com/apparebit/analog/blob/master/docs/grammar.md)
    for the concise summary of analog's fluent interface.


## Overview

Analog builds on two technologies that have become ubiquitous when it comes to
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

  * The metadata sidecar file in JSON format has the same name but with a
    `.json` file extension.

  * `hostnames.json` caches previous DNS lookups of IP addresses, which are by
    far the slowest part of ingesting raw access logs.

When running analog from the command line or invoking `analog.latest()`, analog
first ingests raw monthly logs that have no corresponding enriched log files.
Then, if there is no combined log covering all monthly log files or one of those
files was just updated, analog creates a new combined log and its metadata
sidecar file.

When using the `--clean` command line option or invoking `analog.latest()` with
a truthy `clean` keyword argument, analog starts by deleting all monthly log
files stored in `enriched-logs`, which causes both monthly and combined log
files to be re-generated. You can also deleted these files manually. But
*please*, do *not* delete `access-logs` or `hostnames.json`.


### Log Schema

Analog combines properties parsed from the raw access logs, derived from the
original data, and derived from external databases for domain names, IP
locations, and user agents. The `SCHEMA` mapping in the
[`analog.schema`](https://github.com/apparebit/analog/blob/master/analog/schema.py)
module defines the Pandas schema for the resulting dataframes. It makes use of
several enumerations defined in the
[`analog.label`](https://github.com/apparebit/analog/blob/master/analog/label.py)
module.

Note that analog uses *two* independent databases of user agents to detect bots
— [matomo](https://matomo.org) and [ua-parser](https://github.com/ua-parser).
Each project detects a good number of bots not detected by the other. Hence,
analog's `only.bots()` and `only.humans()` filters take both into account.
analog also fixes a minor misclassification made by ua-parser.

As of July 13, 2023, the latest version of the `ua-parser` package is 0.18.0. It
was released five days before, on July 8, 2023. Since that package saw only two
updates between 2018 and 2022, I did use a forked version, `ua-parser-up2date`.
Its latest version is 0.16.1, which was released on December 16, 2022. Looking
at the two packages' update histories for the last couple of years, the original
`ua-parser` seems preferable again.


### Fluent Grammar

Analog's fluent interface makes use of computed properties as well as methods.
Properties typically distinguish between different types of clauses whereas
methods terminate the clauses. In the grammar below, property and method names
are double quoted. The attribute selector's period is written as `<dot>` and
methods are followed by `()`, with parameters listed in between.

The following grammar summarizes the fluent interface. At the top-level, a
***sentence*** consists of terms to specify (1) selection, (2) grouping and
aggregation, as well as (3) display:

    sentence -> selection grouping-and-aggregation display

The ***selection*** extracts rows that meet certain criteria. It distinguishes
between three kinds of criteria, namely (1) terms that start with the `.only`
property and filter based on attributes of the HTTP protocol, (2) terms that
start with the `.over` property and filter based on datetime, and (3) terms that
invoke `.select()` or `.map()` and thus serve as extension points. You can track
the impact of these filters with the `.count_rows()` method, which appends the
number of rows to the context's list inside a `with analog.fresh_counts()`
block. It is an error to call this method outside such a block. Square brackets
containing a slice, select rows by their numbers.

    selection ->
        | <dot> "only" <dot> protocol  selection
        | <dot> "over" <dot> datetime  selection
        | <dot> "filter" (predicate)   selection
        | <dot> "map" (mapper)         selection
        | <dot> "count_rows" ()        selection
        | [ <slice> ]                  selection
        | 𝜀

The ***protocol*** criterion contains several convenience methods that filter
common protocol values. The `.has()` method is more general and can filter on
the `content_type`, `method`, `protocol` and `status_class` column. Since the
various enumeration constants defined in [the `label`
module](https://github.com/apparebit/analog/blob/master/analog/label.py)
uniquely identify the column, there is no need for also specifying the column
name. In contrast, the `.equals()` method generalizes `.has()` for columns that
do not have a categorical type and therefore requires the column name. Finally,
the `.contains()` method implements a common operation on string-valued data.

    protocol ->
        | "bots" ()
        | "humans" ()
        | "GET" ()
        | "POST" ()
        | ...
        | "markup" ()
        | ...
        | "successful" ()
        | "redirection" ()
        | "client_error" ()
        | "server_error" ()
        |
        | "not_found" ()
        | "equals" (column, value)
        | "one_of" (column, value, value, ...)
        | "contains" (column, value)

The `.bots()` and `.humans()` methods categorize requests based on the `is_bot`
and `is_bot2` properties. They concisely capture two different third-party
classifications of the user agent header. Also see the [hands-on
notebook](https://github.com/apparebit/analog/blob/master/workbook.ipynb).

In contrast to Pandas' expressive and complex operations on times and dates,
analog's ***datetime*** criterion is much simpler — and more limited. It selects
the day, week, or year ending having the last entry in the log as its last day.

either the last calendar day, month, or year  containing the last entry in the
log


day, month, or year ending with the end of the log or an arbitrary range
specified by two Python datetimes or Pandas timestamps. If your analysis focuses
on calendar months, you may find that the `monthly_slice()` and
`monthly_range()` functions in [the `month_in_year`
module](https://github.com/apparebit/analog/blob/master/analog/month_in_year.py)
come in handy. Note that all datetimes and timestamps must have a valid
timezone. It defaults to UTC in analog's own code.

    datetime ->
        | "last_day" ()
        | "last_week" ()
        | "last_year" ()
        | "range" (begin, end_inclusive)

**About extensibility**: Analog is designed to make common log analysis
steps simple and thereby reduce the barrier to entry when using Pandas for log
analysis. But for implementing uncommon analysis steps, you still need to use
Pandas. In particular, you access the wrapped Pandas dataframe or series through
the `.data` property.

Since unwrapping a dataframe, invoking a Pandas method, and then rewrapping the
result is a bit tedious, analog has two extension methods that apply an
arbitrary callable on the wrapped dataframe while also wrapping the result. The
`.select()` method takes a predicate producing a boolean series and the `.map()`
method takes transformation producing another dataframe.

There are three options for ***grouping and aggregation***: a rate and metric,
just a metric by itself, or an explicit bypass of metrics with the `.just`
property. Requiring explicit bypass arguably is less elegant than just omitting
unnecessary clauses. But it also keeps the implementation simpler and hence won
out.

    grouping-and-aggregation ->
        | rate <dot> metric
        | <dot> metric
        | <dot> "just"

A ***rate*** is indicated by the `.monthly` property. So far, I haven't seen the
need to add more options.

    rate -> <dot> "monthly"

Currently supported ***metrics*** are (1) the number of requests, (2) the value
counts for a given column, and (3) the unique values for a given column. The
`.status_classes()` and `.content_types()` methods are convenient aliases for
specific value counts. The `unique_values()` method makes little sense as a rate
and hence is only supported without a preceding `.monthly` property.

    metric ->
        | <dot> "requests" ()
        | <dot> "content_types" ()
        | <dot> "status_classes" ()
        | <dot> "value_counts" (column)
        | <dot> "unique_values" (column)

**About result types**: The result of a selection always is another wrapped
Pandas dataframe. However, if the grouping and aggregation is just a metric
*without* rate, the result of `.requests()` is an integer value that terminates
the fluent expression. Other metrics *without* rate such as `.value_counts()`
and `.unique_values()` produce a wrapped Pandas *series*. If the grouping and
aggregation *includes the rate*, the result of `.requests()` is a wrapped Pandas
*series*. Other metrics *with* rate produce a wrapped Pandas dataframe.

The ***display*** formats, prints, or plots the data. The `.format()` method
converts the wrapped series or dataframe into lines of text. It terminates the
fluent sentence to return the result. `.count_rows()` appends the number of rows
to the context inside a `with analog.fresh_counts()` block, whereas square
brackets containing a slice pick rows by their numbers. `.print()` displays the
data as text and  `.plot()` as a graph.

    display ->
        | <dot> "format" ()
        | <dot> "count_rows" ()        display
        | [ <slice> ]                  display
        | <dot> "print" (rows = None)  display
        | <dot> "plot" (**kwargs)      display
        | <dot> "also" ()              sentence
        | <dot> "done" ()
        | 𝜀

Finally, `.also()` starts another sentence, as long as the wrapped data is a
dataframe, and `.done()` terminates the sentence. Since it returns `None`, the
latter method suppresses the display of a series or dataframe in Jupyter
notebooks.


### Fluent Implementation

The implementation generally follows the grammar. A class implementing a clause
typically has the same name as the corresponding nonterminal, though the name is
CamelCased and prefixed with `Fluent`. All classes representing nonterminals
inherit from the same abstract base class `FluentTerm`, which holds the wrapped
state and provides convenient, private methods for creating new subclass
instances. Since, as described above, some statistics result in series instead
of dataframes, that base class and `FluentDisplay` are generic.


#### *Cool Features*

Three features of the implementation [stand out, especially in a
notebook](https://github.com/apparebit/analog/blob/master/docs/hands-on.ipynb):

  * Wrapped series and dataframes display as HTML tables in Jupyter, when
    invoking `.print()` and when becoming a cell's value.
  * When the fluent grammar generates new wrapped series, it makes sure that the
    series have meaningful index and data names.
  * Wrapped series and dataframes support slicing by row numbers, so you can
    throttle the amount of data displayed in a notebook or interactive shell,
    even when relying on the notebook for doing the displaying.


#### *Entry Points*

The main entry point for fluent analysis is:

    def analyze(frame: pd.DataFrame) -> FluentSentence: ...

It returns an instance of `FluentSentence`. A second function recombines several
wrapped or unwrapped series into a dataframe again, notably for plotting:

    def merge(
      *series: FluentTerm[pd.Series] | pd.Series,
      **named_series: FluentTerm[pd.Series] | pd.Series,
    ) -> FluentSentence:

The function returns a wrapped dataframe that combines all series given as
arguments. For series passed with keyword arguments, it also renames the series
to the keywords.

The `count_rows()` method supported by `FluentSentence` and `FluentDisplay`
requires a context provides with a list for those counts. You create the context
through a `with fresh_counts() as counts` statement.

Happy, happy, joy, joy! 😎

---

© 2022 [Robert Grimm](https://apparebit.com).
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.
[GitHub](https://github.com/apparebit/analog).

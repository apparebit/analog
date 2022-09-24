# analog's fluent grammar

The following grammar describes the entirety of analog's fluent analysis API. It
directly translates into code. For example, the first alternative of `selection`
combined with the first alternative of `protocol` and the last alternative of
`selection` translates into `.only.bots()`, an expression to filter out requests
not made by bots. In translation, `<dot>` becomes a literal dot `.` and `𝜀`
means "nothing." Parentheses are not used for grouping in the grammar; they
specify method arguments and hence remain during translation.

```py
sentence -> selection grouping-plus-aggregation display

selection ->
    | <dot> "only"       <dot> protocol  selection
    | <dot> "over"       <dot> datetime  selection
    | <dot> "select"     (predicate)     selection
        # Delay evaluation of filters until data is needed
    | <dot> "map"        (mapper)        selection
        # Implement more general transformation
    | <dot> "count_rows" ()              selection
        # Append row count to `with analog.fresh_counts()`
    | 𝜀

protocol ->  # Filter on HTTP protocol
    | "bots" ()
    | "humans" ()
    | "GET" ()
    | "POST" ()
    | "markup" ()
    | "successful" ()
    | "redirection" ()
    | "client_error" ()
    | "server_error" ()
    | "not_found" ()
    | "has" (enum-constant)
    | "equals" (column, value)
    | "contains" (column, value)

datetime ->  # Filter on time ranges
    | "last_day" ()
    | "last_month" ()
    | "last_year" ()
    | "range" (begin, end_inclusive)

grouping-plus-aggregation ->
    | <dot> "monthly" <dot> metric
        # Return series from .monthly.requests()
    | <dot> metric
        # Return int from .requests(),
        # Return series from other methods
    | <dot> "just"
        # Bypass metric with no-op

metric ->
    | "requests" ()
    | "content_types" ()
    | "status_classes" ()
    | "value_counts" (column)
    | "unique_values" (column)  # Not after .monthly

display ->
    | <dot> "format"      ()
        # Return list of strings
    | <dot> "count_rows"  ()                  display
        # Append row count to `with analog.fresh_counts()`
    | <dot> "print"       (row_count = None)  display
    | <dot> "plot"        (**kwargs)          display
        # Display data textually or graphically
    | <dot> "also"        ()                  sentence
        # Start next sentence with wrapped dataframe
    | 𝜀
```

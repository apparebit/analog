# Analog(ue)

When mingling with other Python packages in the [package
index](https://pypi.org/project/analogue/), this package goes by the name
**analogue**. That's also the name recognized by pip when you try to install it.
But in its native habitat on [GitHub](https://github.com/apparebit/analog) and
in the wild, after it has been released onto your computer, this package goes by
the shorter and friendlier **analog**.

The latter spelling also comes closer to this package's purpose, **ana**lyzing
**log**s. In particular, analog is designed for the offline, hands-on
exploration of monthly access logs. To that end, analog parses and enriches one
monthly log at a time before converting the data to a
[Pandas](https://pandas.pydata.org) dataframe and storing it as a
[Parquet](https://parquet.apache.org) file. Once all monthly logs have been
processed, analog also creates a single, combined dataframe, stored as Parquet,
and corresponding summary, stored as JSON.

Analog stores its data in the `data` directory:

  * `data/access-logs` contains the raw monthly access logs;
  * `data/enriched-logs` contains the parsed and enriched monthly logs;
  * `data` contains the combined log;
  * `data/location_db` contains IP location databases;
  * `data/hostnames.json` is the cache of hostname lookups.

Analog starts with Apache's access log and extracts the following fields from
each line:

  * `client_address`
  * `timestamp`
  * `method`
  * `path`
  * `query`
  * `protocol`
  * `status`
  * `size`
  * `referrer`
  * `user_agent`
  * `server_address`
  * `server_name`

To simplify analysis, analog then adds these derived fields:

  * `content_type`
  * `cool_path`
  * `referrer_scheme`
  * `referrer_host`
  * `referrer_path`
  * `status_class`

Finally, analog consults DNS as well as databases of user agents and IP address
locations to add these fields:

  * `client_name`
  * `client_longitude`
  * `client_city`
  * `client_country`
  * `agent_family`
  * `agent_version`
  * `os_family`
  * `os_version`
  * `device_family`
  * `device_brand`
  * `device_model`
  * `is_bot`

After a change to the log parser or enrichment logic, it becomes necessary to
re-parse and re-enrich the raw access logs. Analog is fast enough to do so for a
few years' worth of monthly logs in seconds. That is largely thanks to its
persistent cache of hostnames in `hostnames.json`. Without that, analog runs
*much* slower.

---

© 2022 [Robert Grimm](https://apparebit.com).
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license.
[GitHub](https://github.com/apparebit/analog).

# Metabase BigQuery Alt Driver (Community-Supported)

Alternative Big Query Driver for Metabase (forked from original driver), capable of using multiple datasets.


## Obtaining the BigQuery Driver

### Where to find it

[Click here](https://github.com/pipefy/metabase-bigquery/releases/latest) to view the latest release of the Metabase BigQuery driver; click the link to download `bigquery-alt.metabase-driver.jar`.

You can find past releases of the BigQuery driver [here](https://github.com/pipefy/metabase-bigquery/releases).


### How to Install it

Metabase will automatically make the BigQuery driver if it finds the driver JAR in the Metabase plugins directory when it starts up.
All you need to do is create the directory (if it's not already there), move the JAR you just downloaded into it, and restart Metabase.

By default, the plugins directory is called `plugins`, and lives in the same directory as the Metabase JAR.

For example, if you're running Metabase from a directory called `/app/`, you should move the BigQuery driver JAR to `/app/plugins/`:

```bash
# example directory structure for running Metabase with Vertica support
/app/metabase.jar
/app/plugins/bigquery-alt.metabase-driver.jar
```

If you're running Metabase from the Mac App, the plugins directory defaults to `~/Library/Application Support/Metabase/Plugins/`:

```bash
# example directory structure for running Metabase Mac App with Vertica support
/Users/camsaul/Library/Application Support/Metabase/Plugins/bigquery-alt.metabase-driver.jar
```

If you are running the Docker image or you want to use another directory for plugins, you should specify a custom plugins directory by setting the environment variable `MB_PLUGINS_DIR`.



## Building the BigQuery Driver Yourself

### Prereqs: Install Metabase locally, compiled for building drivers

```bash
cd /path/to/metabase/source
lein install-for-building-drivers
```

### Build it

```bash
cd /path/to/bigquery-alt-driver
lein clean
DEBUG=1 LEIN_SNAPSHOTS_IN_RELEASE=true lein uberjar
```

This will build a file called `target/uberjar/bigquery-alt.metabase-driver.jar`; copy this to your Metabase `./plugins` directory.


This README was lifted from CrateDB Driver (https://github.com/metabase/crate-driver)[https://github.com/metabase/crate-driver]

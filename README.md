# OSM Fastly Logs

This is ofastlylog, the software that processes OpenStreetMap logs from Fastly.

## Install

Packaged versions of ofastlylog can be installed like any other python software. To do so in a virtual environment do

```sh
python3 -m venv venv
venv/bin/pip install ofastlylog
venv/bin/ofastlylog --help
```

Development is done using uv.

```sh
uv sync
uv run ofastlylog --help
```

Development could also be done using a venv and pip

```sh
python3 -m venv venv
venv/bin/pip install -e .
venv/bin/ofastlylog --help
```

## Usage

AWS credentials need to be set up for ofastlylog to access the service logs. In a typical setup this is done with

```sh
export AWS_PROFILE=osm-service-logs
```

ofastlylog has two commands right now: one to create tables, and another to perform hourly tasks.

All commands take a list of service tables to create or process. Specify these with `--<servicename> <tabletype>`, e.g. `--tile success` for the tile.openstreetmap.org success table. See [Table Types](#Table_Types) for a full list of combinations.


### `ofastlylog tools`

Contains tools for creating and working with setup tables

#### `ofastlylog tools create-table`

Creates tables in athena.

#### `ofastlylog tools check-hour`

TODO: Check if a particular hour has data in the table

### `ofastlylog process`
Peform log generation and processing

#### `ofastlylog process hourly`
Perform log processing intended to be run hourly

## Table types

|   Table     | `--tile` | `--vector` | `--nominatim` |
|------------:|----------|------------|---------------|
| `base`      |        ✓ |          ✓ |             ✓ |
| `success`   |        ✓ |          ✓ |             ✓ |
| `minimized` | Not implemented | not impelemented | not impelemented |

## Architecture

Several OSMF-hosted services use Fastly as a CDN. Fastly is set up to deposit compressed log files in a s3 bucket. The log files contain [line-delimited JSON objects](https://quic.fastly.com/documentation/guides/integrations/streaming-logs/useful-log-formats/#json) deposited every 10 minutes.

Because compressed text is slow to read, these files are processed with `ofastlylog process hourly`. This creates a number of tables. All tables are stored as Parquet data with hourly partitioning.

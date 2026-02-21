import copy
from dataclasses import dataclass
import datetime

import pyathena.connection
import pyathena.pandas.cursor


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    comment: str | None = None
    success_sql: str | None = None


def create_columns_sql(columns: list[Column]) -> str:
    """
    SQL to turn a list of columns into the SQL to generate them

    :param columns: Columns to be created
    :return: SQL to generate the columns
    """
    return ",\n".join([f"{c.name} {c.type} COMMENT '{c.comment}'" for c in columns])


class Service:
    schema: str = "logs"
    base_name: str
    success_name: str

    """List of columns to create"""
    base_columns: list[Column]
    success_columns: list[Column]

    """Table comment"""
    base_comment: str
    success_comment: str

    """S3 location"""
    base_location: str
    success_location: str

    success_filter_sql: str = "status IN (200, 206, 304)\n"

    def __init__(
        self, connection: pyathena.connection.Connection[pyathena.pandas.cursor.PandasCursor]
    ) -> None:
        self.connection = connection
        # These columns are common to all success tables
        self.success_columns = [
            Column(name="time_m", type="int", comment="Minutes for start of request"),
            Column(name="time_s", type="float", comment="Seconds for start of request"),
            Column(name="ip", type="string", comment="Client IP address"),
            Column(name="host", type="string", comment="Server hostname"),
            Column(name="status", type="smallint", comment="Response HTTP Status"),
            Column(name="hit", type="boolean", comment="Cache hit?"),
            Column(name="size", type="int", comment="Response body size in bytes"),
            Column(name="ttfb", type="float", comment="Time to first byte in seconds"),
            Column(name="backend", type="string", comment="Backend server for response"),
            Column(name="referer", type="string", comment="HTTP Referer Header"),
            Column(name="useragent", type="string", comment="HTTP User-Agent Header"),
            Column(name="s_ch", type="string", comment="HTTP Sec-CH-UA Header"),
            Column(name="s_fetchsite", type="string", comment="HTTP Sec-Fetch-Site Header"),
            Column(name="origin", type="string", comment="HTTP Origin Header"),
            Column(name="reqwith", type="string", comment="HTTP X-Requested-With Header"),
            Column(name="accept", type="string", comment="HTTP Accept Header"),
            Column(name="acceptlanguage", type="string", comment="HTTP Accept-Language Header"),
            Column(name="reqs", type="string", comment="Number of requests in HTTP connection"),
            Column(name="sigsci", type="string", comment="NGWAF signals"),
            Column(name="ja4t", type="string", comment="JA4T fingerprint"),
            Column(name="ja4", type="string", comment="JA4 fingerprint"),
            Column(name="ja4l", type="string", comment="JA4L fingerprint"),
            Column(name="ja4h", type="string", comment="JA4H fingerprint"),
            Column(name="asn", type="int", comment="Client ASN"),
            Column(name="pop", type="string", comment="Fastly POP"),
            Column(name="country", type="string", comment="Client country"),
            Column(name="tz", type="int", comment="Client timezone offset"),
        ]

        # The common columns in the success table also need to be in the base table
        self.base_columns = copy.deepcopy(self.success_columns)
        # The success table may convert the path into some other form, e.g. x, y, z for tiles
        # so we only put it in the base table
        self.base_columns += [Column(name="path", type="string", comment="Path requested")]

    def create_base_table(self) -> None:
        """
        Creates the table for the raw logs from Fastly
        """
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_base_table_sql())

    def create_success_table(self) -> None:
        """
        Creates the table for processed success logs from fastly
        """
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_success_table_sql())

    def create_base_table_sql(self) -> str:
        # TODO: assert that self.location ends with /
        # ruff: disable[F541]
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.base_name} (\n"""
            + f"""{create_columns_sql(self.base_columns)})\n"""
            + f"""COMMENT '{self.base_comment}'\n"""
            + f"""PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + f"""ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n"""
            + f"""STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n"""
            f"""OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"""
            + f"""LOCATION '{self.base_location}'\n"""
            + f"""TBLPROPERTIES (\n"""
            + f"""'has_encrypted_data'='false',\n"""
            + f"""'storage.location.template'='{self.base_location}"""
            + "${year}/${month}/${day}/${hour}/',\n"
            + f"""'projection.enabled'='true',\n"""
            + f"""'projection.year.type'='integer',\n"""
            + f"""'projection.year.range'='2025,2040',\n"""
            + f"""'projection.month.type'='integer',\n"""
            + f"""'projection.month.range'='1,12',\n"""
            + f"""'projection.month.digits'='2',\n"""
            + f"""'projection.day.type'='integer',\n"""
            + f"""'projection.day.range'='1,31',\n"""
            + f"""'projection.day.digits'='2',\n"""
            + f"""'projection.hour.type'='integer',\n"""
            + f"""'projection.hour.range'='0,23',\n"""
            + f"""'projection.hour.digits'='2')"""
        )
        # ruff: enable[F541]
        return sql

    def create_success_table_sql(self) -> str:
        # TODO: assert that self.location ends with /
        # ruff: disable[F541]
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.success_name} (\n"""
            + f"""{create_columns_sql(self.success_columns)})\n"""
            + f"""COMMENT '{self.success_comment}'\n"""
            + f"""PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + f"""ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"""
            + f"""STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\n"""
            f"""OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"""
            + f"""LOCATION '{self.success_location}'\n"""
            + f"""TBLPROPERTIES (\n"""
            + f"""'has_encrypted_data'='false',\n"""
            + f"""'storage.location.template'='{self.success_location}"""
            + "${year}/${month}/${day}/${hour}/',\n"
            + f"""'projection.enabled'='true',\n"""
            + f"""'projection.year.type'='integer',\n"""
            + f"""'projection.year.range'='2025,2040',\n"""
            + f"""'projection.month.type'='integer',\n"""
            +
            # INSERT INTO does not work with projection and digits, so we have slightly different locations on S3.
            f"""'projection.month.range'='1,12',\n"""
            + f"""'projection.day.type'='integer',\n"""
            + f"""'projection.day.range'='1,31',\n"""
            + f"""'projection.hour.type'='integer',\n"""
            + f"""'projection.hour.range'='0,23',\n"""
            + f"""'parquet.compression'='ZSTD')"""
        )
        # ruff: enable[F541]
        return sql

    def process_hourly_success(self, date: datetime.datetime, hours: int) -> None:
        """
        Process the logs for a given hour and write to the success table
        """
        if not self.check_hour(self.base_name, date + datetime.timedelta(hours=1)):
            raise ValueError(
                f"No data more recent than {date + datetime.timedelta(hours=1)} has arrived."
                + "The previous hour cannot be processed until the next hour's data starts."
            )

        missing = self.get_missing_partitions(self.success_name, date, hours)
        if missing == []:
            print(f"All data for the last {hours} hours is already processed")
            return
        with self.connection.cursor() as cursor:
            for d in self.get_missing_partitions(self.success_name, date, hours):
                print(f"Processing {d} for {self.success_name}")
                cursor.execute(self.process_hourly_success_sql(d))

    def process_hourly_success_sql(self, date: datetime.datetime) -> str:
        columns = [c.success_sql or c.name for c in self.success_columns]
        columns += ["year", "month", "day", "hour"]
        # ruff: disable[F541]
        return (
            f"""INSERT INTO {self.schema}.{self.success_name}\n"""
            + f"""SELECT\n"""
            + ",\n".join(columns)
            + "\n"
            + f"""FROM {self.schema}.{self.base_name}\n"""
            + f"""WHERE year={date.year} AND month={date.month} AND day={date.day} AND hour={date.hour}\n"""
            + f"""AND {self.success_filter_sql}\n"""
        )
        # ruff: enable[F541]

    def check_hour(self, table: str, date: datetime.datetime) -> bool:
        """
        Check if a given hour has data in the base table

        :return: True if there is data for the hour, False otherwise
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self.schema}.{table}\n"
                f"WHERE year={date.year} AND month={date.month} AND day={date.day} AND hour={date.hour}\n"
                "LIMIT 1"
            )
            return len(cursor.fetchall()) > 0

    def get_missing_partitions(
        self, table: str, date: datetime.datetime, hours: int = 1
    ) -> list[datetime.datetime]:
        """
        Find what partitions are missing from a table

        Starts at date and works backwards hours hours
        """
        result: list[datetime.datetime] = []
        for h in range(hours):
            d = date - datetime.timedelta(hours=h)
            if not self.check_hour(table, d):
                result.append(d)

        return result


class TileService(Service):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)


class RasterTileService(TileService):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.base_name = "fastly_raster_logs_v20"
        self.base_location = "s3://openstreetmap-fastly-raster-logs/tile/v20/"
        self.base_comment = "Fastly logs for tile.openstreetmap.org"
        self.base_columns += [
            Column(name="ratelimit", type="tinyint", comment="Rate limiting mode applied")
        ]

        self.success_name = "fastly_raster_success_logs_v4"
        self.success_location = "s3://openstreetmap-fastly-raster-processed-logs/success/v4/"
        self.success_comment = "Fastly logs for tile.openstreetmap.org successful requests"
        tile_regexp = r"^/(\d{1,2})/(\d{1,6})/(\d{1,6})"
        self.success_columns = [
            Column(
                name="z",
                type="tinyint",
                comment="Zoom level of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 1) AS integer) AS z",
            ),
            Column(
                name="x",
                type="integer",
                comment="x of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 2) AS integer) AS x",
            ),
            Column(
                name="y",
                type="integer",
                comment="y of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 3) AS integer) AS y",
            ),
            *self.success_columns,
            Column(name="ratelimit", type="tinyint", comment="Rate limiting mode applied"),
        ]
        self.success_filter_sql = (
            self.success_filter_sql + f"AND regexp_like(path, '{tile_regexp}')"
        )


class VectorTileService(TileService):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.base_name = "fastly_vector_logs_v2"
        self.base_location = "s3://openstreetmap-fastly-vector-logs/vector/v2/"
        self.base_comment = "Fastly logs for vector.openstreetmap.org"

        self.success_name = "fastly_vector_success_logs_v2"
        self.success_location = "s3://openstreetmap-fastly-vector-processed-logs/vector/v2/"
        self.success_comment = "Fastly logs for vector.openstreetmap.org successful requests"
        tile_regexp = r"^/([^/]+)/(\d{1,2})/(\d{1,6})/(\d{1,6})"
        self.success_columns = [
            # These differ from raster tiles because we serve multiple tilesets
            Column(
                name="tileset",
                type="string",
                comment="Tileset served",
                success_sql=f"regexp_extract(path, '{tile_regexp}', 1) AS tileset",
            ),
            Column(
                name="z",
                type="tinyint",
                comment="Zoom level of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 2) AS integer) AS z",
            ),
            Column(
                name="x",
                type="integer",
                comment="x of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 3) AS integer) AS x",
            ),
            Column(
                name="y",
                type="integer",
                comment="y of tile",
                success_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 4) AS integer) AS y",
            ),
            *self.success_columns,
        ]
        self.success_filter_sql = (
            self.success_filter_sql + f"AND regexp_like(path, '{tile_regexp}')"
        )


class NominatimService(Service):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.base_name = "fastly_nominatim_logs_v3"
        self.base_location = "s3://openstreetmap-fastly-nominatim-logs/nominatim/v3/"
        self.base_comment = "Fastly logs for nominatim.openstreetmap.org"
        self.base_columns += [
            Column(name="query", type="string", comment="Query string of request")
        ]

        self.success_name = "fastly_nominatim_success_logs_v1"
        self.success_location = "s3://openstreetmap-fastly-nominatim-processed-logs/success/v1/"
        self.success_comment = "Fastly logs for nominatim.openstreetmap.org successful requests"

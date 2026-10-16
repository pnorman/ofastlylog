import datetime

from .base import (
    Column,
    Service,
    create_columns_sql,
    date_condition,
    date_params,
    ymdh_tablestorage,
)

__all__ = [
    "RasterTileService",
    "VectorTileService",
]


class TileService(Service):
    location_name: str

    location_columns: list[Column]

    location_comment: str

    # s3 location of location table
    location_location: str

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.aggregated_columns = [
            Column(name="z", type="tinyint", comment="Zoom level of tile"),
            *self.aggregated_columns,
        ]

        self.location_columns = [
            Column(
                name="requests",
                type="bigint",
                comment="Number of requests",
                generation_sql="COUNT(*)",
            ),
            Column(
                name="distinct_ips",
                type="bigint",
                comment="Number of distinct IPs",
                generation_sql="COUNT(DISTINCT ip)",
            ),
            Column(
                name="size",
                type="bigint",
                comment="Total body size",
                generation_sql="SUM(size)",
            ),
            Column(name="z", type="tinyint", comment="Zoom level of tile"),
            Column(name="x", type="int", comment="X coordinate of tile"),
            Column(name="y", type="int", comment="Y coordinate of tile"),
            Column(name="hit", type="boolean", comment="Cache hit?"),
            Column(name="pop", type="string", comment="Fastly POP"),
            Column(name="country", type="string", comment="Client country"),
            Column(name="tz", type="int", comment="Client timezone offset"),
        ]

    def create_location_table(self) -> None:
        """
        Creates the table for aggregated logs from fastly
        """
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_location_table_sql())

    def create_location_table_sql(self) -> str:
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.location_name} (\n"""
            + f"""{create_columns_sql(self.location_columns)})\n"""
            + f"""COMMENT '{self.location_comment}'\n"""
            + """PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + """ROW FORMAT\n"""
            + """SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"""
            + """STORED AS\n"""
            + """INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\n"""
            + """OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"""
            + ymdh_tablestorage(self.location_location)
        )
        return sql

    def process_hourly_location(self, date: datetime.datetime, hours: int) -> None:
        """
        Process the logs for a given hour and write to the location table
        """
        # No check is required for the next hour because under normal conditions
        # the success table will either be populated or not, not partially like
        # the base table can be.

        missing = self.get_missing_partitions(self.location_name, date, hours)
        if missing == []:
            print(f"All data for the last {hours} hours is already been location processed")
            return
        with self.connection.cursor() as cursor:
            # TODO: Reuse variable above.
            for d in self.get_missing_partitions(self.location_name, date, hours):
                print(f"Processing {d} for {self.location_name}")
                cursor.execute(self.process_hourly_location_sql(d), date_params(d))

    def process_hourly_location_sql(self, date: datetime.datetime) -> str:
        columns: list[str] = []
        group_columns: list[str] = []
        for col in self.location_columns:
            if col.generation_sql is not None:
                columns.append(f'''{col.generation_sql} AS "{col.name}"''')
            else:
                columns.append(f"{col.name}")
                group_columns.append(f"{col.name}")

        columns += ["year", "month", "day", "hour"]
        group_columns += ["year", "month", "day", "hour"]

        insert_columns = [col.name for col in self.location_columns] + [
            "year",
            "month",
            "day",
            "hour",
        ]

        return (
            f"INSERT INTO {self.schema}.{self.location_name}\n"
            + "(\n"
            + ",\n".join(insert_columns)
            + "\n)\n"
            + "SELECT\n"
            + ",\n".join(columns)
            + "\n"
            + f"FROM {self.schema}.{self.success_name}\n"
            + f"WHERE {date_condition(date)}\n"
            # This GROUP BY has a hidden requirement. If the data
            # matching the WHERE clause is too large, Athena will
            # fail with resource limits.
            # TODO: Add a way to work around this
            + "GROUP BY\n"
            + ",\n".join(group_columns)
        )


class RasterTileService(TileService):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.base_name = "fastly_raster_logs_v20"
        self.base_location = "s3://openstreetmap-fastly-raster-logs/tile/v20/"
        self.base_comment = "Fastly logs for tile.openstreetmap.org"
        self.base_columns = [
            *self.base_columns,
            Column(name="ratelimit", type="tinyint", comment="Rate limiting mode applied"),
            Column(name="totp", type="string", comment="OSM TOTP auth token"),
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
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 1) AS tinyint)",
            ),
            Column(
                name="x",
                type="integer",
                comment="x of tile",
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 2) AS integer)",
            ),
            Column(
                name="y",
                type="integer",
                comment="y of tile",
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 3) AS integer)",
            ),
            *self.success_columns,
            Column(name="ratelimit", type="tinyint", comment="Rate limiting mode applied"),
            Column(name="totp", type="string", comment="OSM TOTP auth token"),
        ]
        self.success_filter_sql += f"AND regexp_like(path, '{tile_regexp}'\n)"

        self.aggregated_name = "fastly_raster_aggregated_logs_v1"
        self.aggregated_location = "s3://openstreetmap-fastly-raster-aggregated-logs/aggregated/v1/"
        self.aggregated_comment = "Aggregated logs for tile.openstreetmap.org"

        self.location_name = "fastly_raster_location_logs_v4"
        self.location_location = "s3://openstreetmap-fastly-raster-location-logs/location/v4/"
        self.location_comment = "Location logs for tile.openstreetmap.org"


class VectorTileService(TileService):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.base_name = "fastly_vector_logs_v2"
        self.base_location = "s3://openstreetmap-fastly-vector-logs/vector/v2/"
        self.base_comment = "Fastly logs for vector.openstreetmap.org"

        self.success_name = "fastly_vector_success_logs_v2"
        self.success_location = "s3://openstreetmap-fastly-vector-processed-logs/vector/v2/"
        self.success_comment = "Fastly logs for vector.openstreetmap.org successful requests"
        # Match the format /tileset/z/x/y.foo and cap z, x, y to sane ranges
        tile_regexp = r"^/([^/]+)/(\d{1,2})/(\d{1,6})/(\d{1,6})"
        self.success_columns = [
            # These differ from raster tiles because we serve multiple tilesets
            Column(
                name="tileset",
                type="string",
                comment="Tileset served",
                generation_sql=f"regexp_extract(path, '{tile_regexp}', 1)",
            ),
            Column(
                name="z",
                type="tinyint",
                comment="Zoom level of tile",
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 2) AS tinyint)",
            ),
            Column(
                name="x",
                type="integer",
                comment="x of tile",
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 3) AS integer)",
            ),
            Column(
                name="y",
                type="integer",
                comment="y of tile",
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 4) AS integer)",
            ),
            *self.success_columns,
        ]
        self.success_filter_sql += f"AND regexp_like(path, '{tile_regexp}'\n)"

        self.aggregated_name = "fastly_vector_aggregated_logs_v1"
        self.aggregated_location = "s3://openstreetmap-fastly-vector-aggregated-logs/aggregated/v1/"
        self.aggregated_comment = "Aggregated logs for vector.openstreetmap.org"

        # TODO: This could be moved to the TileService base class except for the tileset column
        self.aggregated_columns = [
            Column(name="tileset", type="string", comment="Tileset served"),
            *self.aggregated_columns,
        ]

        self.location_name = "fastly_vector_location_logs_v1"
        self.location_location = "s3://openstreetmap-fastly-vector-location-logs/location/v1/"
        self.location_comment = "Location logs for vector.openstreetmap.org"

        self.location_columns = [
            Column(name="tileset", type="string", comment="Tileset served"),
            *self.location_columns,
        ]

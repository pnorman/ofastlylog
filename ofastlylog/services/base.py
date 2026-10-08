import copy
import datetime
from dataclasses import dataclass

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


def date_condition(date: datetime.datetime) -> str:
    """
    Generate a SQL WHERE condition for a specific date

    :param date: Date to generate the condition for
    :return: SQL condition for the date
    """
    return "year=%(year)i AND month=%(month)i AND day=%(day)i AND hour=%(hour)i"


def date_params(date: datetime.datetime) -> dict[str, int]:
    """
    Generate a dict of date parameters to be passed to cursor.execute
    :param date: Date to generate the parameters for"""
    return {
        "year": date.year,
        "month": date.month,
        "day": date.day,
        "hour": date.hour,
    }


def ymdh_tablestorage(location: str) -> str:
    """
    Generate the location and properties for a table with standard partitions
    """
    return (
        f"""LOCATION '{location}'"""
        + """TBLPROPERTIES (\n"""
        + """'has_encrypted_data'='false',\n"""
        + f"""'storage.location.template'='{location}"""  # no newline or comma
        + """${year}/${month}/${day}/${hour}/',\n"""  # not a f-string because its a s3 template
        + """'projection.enabled'='true',\n"""
        + """'projection.year.type'='integer',\n"""
        + """'projection.year.range'='2025,2040',\n"""
        + """'projection.month.type'='integer',\n"""
        + """'projection.month.range'='1,12',\n"""
        + """'projection.month.digits'='2',\n"""
        + """'projection.day.type'='integer',\n"""
        + """'projection.day.range'='1,31',\n"""
        + """'projection.day.digits'='2',\n"""
        + """'projection.hour.type'='integer',\n"""
        + """'projection.hour.range'='0,23',\n"""
        + """'projection.hour.digits'='2')"""
    )


class Service:
    schema: str = "logs"
    base_name: str
    success_name: str
    aggregation_name: str

    """List of columns to create"""
    base_columns: list[Column]
    success_columns: list[Column]
    aggregation_columns: list[Column]

    """Table comment"""
    base_comment: str
    success_comment: str
    aggregation_comment: str

    """S3 location"""
    base_location: str
    success_location: str
    aggregation_location: str

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

    def create_aggregation_table(self) -> None:
        """
        Creates the table for aggregated logs from fastly
        """
        with self.connection.cursor() as cursor:
            cursor.execute(self.create_aggregation_table_sql())

    def create_base_table_sql(self) -> str:
        # TODO: assert that self.location ends with /
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.base_name} (\n"""
            + f"""{create_columns_sql(self.base_columns)})\n"""
            + f"""COMMENT '{self.base_comment}'\n"""
            + """PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + """ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n"""
            + """STORED AS\n"""
            + """INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'\n"""
            + """OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n"""
            + ymdh_tablestorage(self.base_location)
        )
        return sql

    def create_success_table_sql(self) -> str:
        # TODO: assert that self.location ends with /
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.success_name} (\n"""
            + f"""{create_columns_sql(self.success_columns)})\n"""
            + f"""COMMENT '{self.success_comment}'\n"""
            + """PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + """ROW FORMAT\n"""
            + """SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"""
            + """STORED AS\n"""
            + """INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\n"""
            + """OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"""
            + ymdh_tablestorage(self.success_location)
        )
        return sql

    def create_aggregation_table_sql(self) -> str:
        sql = (
            f"""CREATE EXTERNAL TABLE {self.schema}.{self.aggregation_name} (\n"""
            + f"""{create_columns_sql(self.aggregation_columns)})\n"""
            + f"""COMMENT '{self.aggregation_comment}'\n"""
            + """PARTITIONED BY (year int, month int, day int, hour int)\n"""
            + """ROW FORMAT\n"""
            + """SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n"""
            + """STORED AS\n"""
            + """INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\n"""
            + """OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n"""
            + ymdh_tablestorage(self.aggregation_location)
        )
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
                cursor.execute(self.process_hourly_success_sql(d), date_params(d))

    def process_hourly_success_sql(self, date: datetime.datetime) -> str:
        columns = [c.success_sql or c.name for c in self.success_columns]
        columns += ["year", "month", "day", "hour"]
        return (
            f"""INSERT INTO {self.schema}.{self.success_name}\n"""
            + """SELECT\n"""
            + ",\n".join(columns)
            + "\n"
            + f"""FROM {self.schema}.{self.base_name}\n"""
            + f"""WHERE {date_condition(date)}\n"""
            + f"""AND {self.success_filter_sql}\n"""
        )

    def check_hour(self, table: str, date: datetime.datetime) -> bool:
        """
        Check if a given hour has data in the base table

        :return: True if there is data for the hour, False otherwise
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self.schema}.{table}\n"
                + f"WHERE {date_condition(date)}\n"
                + "LIMIT 1",
                date_params(date),
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

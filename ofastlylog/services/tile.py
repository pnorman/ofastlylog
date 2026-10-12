from .base import Column, Service

__all__ = [
    "RasterTileService",
    "VectorTileService",
]


class TileService(Service):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)


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
        self.success_filter_sql += f"AND regexp_like(path, '{tile_regexp}\n')"

        self.aggregation_name = "fastly_raster_aggregated_logs_v1"
        self.aggregation_location = (
            "s3://openstreetmap-fastly-raster-aggregated-logs/aggregated/v1/"
        )
        self.aggregation_comment = "Aggregated logs for raster.openstreetmap.org"

        # TODO: This could be moved to the TileService base class
        self.aggregation_columns = [
            Column(
                name="z",
                type="tinyint",
                comment="Zoom level of tile",
            ),
            *self.aggregation_columns,
        ]


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
                generation_sql=f"CAST(regexp_extract(path, '{tile_regexp}', 2) AS integer)",
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
        self.success_filter_sql += f"AND regexp_like(path, '{tile_regexp}\n')"

        self.aggregation_name = "fastly_vector_aggregated_logs_v1"
        self.aggregation_location = (
            "s3://openstreetmap-fastly-vector-aggregated-logs/aggregated/v1/"
        )
        self.aggregation_comment = "Aggregated logs for vector.openstreetmap.org"

        # TODO: This could be moved to the TileService base class except for the tileset column
        self.aggregation_columns = [
            Column(
                name="tileset",
                type="string",
                comment="Tileset served",
            ),
            Column(
                name="z",
                type="tinyint",
                comment="Zoom level of tile",
            ),
            *self.aggregation_columns,
        ]

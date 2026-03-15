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
            Column(name="totp", type="string", comment="OSM TOTP auth token"),
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

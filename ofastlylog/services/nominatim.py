from .base import Column, Service


class NominatimService(Service):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.base_name = "fastly_nominatim_logs_v3"
        self.base_location = "s3://openstreetmap-fastly-nominatim-logs/nominatim/v3/"
        self.base_comment = "Fastly logs for nominatim.openstreetmap.org"
        self.base_columns = [
            Column(name="query", type="string", comment="Query string of request"),
            *self.base_columns,
        ]

        self.success_name = "fastly_nominatim_success_logs_v1"
        self.success_location = "s3://openstreetmap-fastly-nominatim-processed-logs/success/v1/"
        self.success_comment = "Fastly logs for nominatim.openstreetmap.org successful requests"
        self.success_columns = [
            Column(name="path", type="string", comment="Path of request"),
            Column(name="query", type="string", comment="Query string of request"),
            *self.success_columns,
        ]

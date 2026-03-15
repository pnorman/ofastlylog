from .base import Column, Service


class WebsiteService(Service):
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)

        self.base_name = "fastly_website_logs_v3"
        self.base_location = "s3://openstreetmap-fastly-website-logs/website/v3/"
        self.base_comment = "Fastly logs for website.openstreetmap.org"
        self.base_columns = [
            Column(name="query", type="string", comment="Query string of request"),
            Column(name="oauth", type="boolean", comment="Has bearer token"),
            *self.base_columns,
        ]

        # self.success_name = "fastly_website_success_logs_v1"
        # self.success_location = "s3://openstreetmap-fastly-website-processed-logs/success/v1/"
        # self.success_comment = "Fastly logs for website.openstreetmap.org successful requests"
        # self.success_columns = [
        #     Column(name="path", type="string", comment="Path of request"),
        #     Column(name="query", type="string", comment="Query string of request"),
        #     *self.success_columns,
        # ]

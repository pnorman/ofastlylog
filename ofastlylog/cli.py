import datetime
from typing import Annotated

import pyathena
import pyathena.pandas.cursor
import typer

from ofastlylog.services import Service, RasterTileService, VectorTileService, NominatimService

app = typer.Typer()

tools_app = typer.Typer()
process_app = typer.Typer()
app.add_typer(tools_app, name="setup")
app.add_typer(process_app, name="process")

DEFAULT_REGION = "eu-north-1"
DEFAULT_WORK_GROUP = "primary"

# TODO: check that there are no unknown table values in the lists and warn
# perhaps turn them into a set, remove each from set when processed, and
# warn if any left?
@tools_app.command()
def create_table(raster: Annotated[list[str], typer.Option()] = [],
                 vector: Annotated[list[str], typer.Option()] = [],
                 nominatim: Annotated[list[str], typer.Option()] = [],
                 region: str = DEFAULT_REGION,
                 work_group: str = DEFAULT_WORK_GROUP) -> None:
    service: Service

    if raster != []:
        with pyathena.connect(region_name=region,
                              cursor_class=pyathena.pandas.cursor.PandasCursor,
                              work_group=work_group) as conn:
            service = RasterTileService(conn)
            if "base" in raster:
                service.create_base_table()
            if "success" in raster:
                service.create_success_table()

    if vector != []:
        with pyathena.connect(region_name=region,
                              cursor_class=pyathena.pandas.cursor.PandasCursor,
                              work_group=work_group) as conn:
            service = VectorTileService(conn)
            if "base" in vector:
                service.create_base_table()
            if "success" in vector:
                service.create_success_table()

    if nominatim != []:
        with pyathena.connect(region_name=region,
                              cursor_class=pyathena.pandas.cursor.PandasCursor,
                              work_group=work_group) as conn:
            service = NominatimService(conn)
            if "base" in nominatim:
                service.create_base_table()
            if "success" in nominatim:
                raise NotImplementedError("Nominatim success table not implemented yet")
                # service.create_success_table()

@process_app.command()
def hourly(raster: Annotated[list[str], typer.Option()] = [],
                   vector: Annotated[list[str], typer.Option()] = [],
                   date: datetime.datetime = (datetime.datetime.now(datetime.timezone.utc) -
                       datetime.timedelta(hours=1)),
                   hours: int = 1,
                   region: str = DEFAULT_REGION,
                   work_group: str = DEFAULT_WORK_GROUP) -> None:
    service: Service

    date = datetime.datetime(date.year, date.month, date.day, date.hour, tzinfo=datetime.timezone.utc)
    print(f"Starting generation at {date} and checking {hours} hours for required processing")
    if raster != []:
        with pyathena.connect(region_name=region,
                              cursor_class=pyathena.pandas.cursor.PandasCursor,
                              work_group=work_group) as conn:
            service = RasterTileService(conn)
            if "success" in raster:
                service.process_hourly_success(date, hours)

    if vector != []:
        with pyathena.connect(region_name=region,
                              cursor_class=pyathena.pandas.cursor.PandasCursor,
                              work_group=work_group) as conn:
            service = VectorTileService(conn)
            if "success" in vector:
                service.process_hourly_success(date, hours)

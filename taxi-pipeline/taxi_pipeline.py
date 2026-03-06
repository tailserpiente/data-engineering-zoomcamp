from __future__ import annotations

from pathlib import Path

import dlt
from dlt.destinations import duckdb
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
DUCKDB_PATH = Path(__file__).with_name("taxi_pipeline.duckdb")


def trips_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_URL,
        },
        "resources": [
            {
                "name": "trips",
                "endpoint": {
                    # Data is served directly from the base URL
                    "path": "",
                    # Pagination: `?page=1`, `?page=2`, ... until an empty list.
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        # API returns a bare list and signals completion with an empty page.
                        # Disable looking for total pages in the response.
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }
    return rest_api_source(config)


def run() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination=duckdb(credentials=str(DUCKDB_PATH)),
        # Put `trips` into the default DuckDB schema.
        dataset_name="main",
        # Re-create tables/state on each run to avoid duplicates while iterating.
        refresh="drop_sources",
        progress="log",
    )

    load_info = pipeline.run(trips_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run()

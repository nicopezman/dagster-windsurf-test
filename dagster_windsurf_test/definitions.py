from dagster import Definitions, load_assets_from_modules

from . import assets
from . import es_assets
from . import resources
# Load all assets from the assets module
all_assets = load_assets_from_modules([assets, es_assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "get_es_conn": resources.get_es_conn.configured(
            {
                "auth": ("admin","admin"),
                "url": "https://demo-opensearch.opendistro.dev",
            }
        ),
        "database": resources.duckdb
    }
)

from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
    ScheduleDefinition,
    EnvVar
)

# Import assets from modules
from . import assets, es_assets
from .country_api import assets as country_assets

# Import resources
from . import resources
from .country_api import resources as country_resources

# Load all assets from modules
all_assets = load_assets_from_modules(
    [assets, es_assets, country_assets],
    group_name="default"
)

# Define jobs
country_data_job = define_asset_job(
    name="country_data_job",
    selection=["raw_countries", "processed_countries", "country_stats"]
)

# Define schedules
country_data_schedule = ScheduleDefinition(
    job=country_data_job,
    cron_schedule="0 0 * * *"  # Daily at midnight
)

defs = Definitions(
    assets=all_assets,
    resources={
        # Existing resources
        "get_es_conn": resources.get_es_conn.configured({
            "auth": ["admin", "admin"],
            "url": "https://demo-opensearch.opendistro.dev"
        }),
        "database": resources.duckdb,
        
        # New country API resource
        "country_api": country_resources.country_api_client
    },
    jobs=[country_data_job],
    schedules=[country_data_schedule]
)

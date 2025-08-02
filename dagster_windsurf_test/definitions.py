from dagster import Definitions, load_assets_from_modules

from . import assets
from . import es_assets

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets, es_assets])

defs = Definitions(
    assets=all_assets,
)

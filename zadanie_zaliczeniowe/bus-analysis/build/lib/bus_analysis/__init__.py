from dagster import Definitions, load_assets_from_modules, FilesystemIOManager, EnvVar
from .resources import WarsawApiResource
from . import assets

all_assets = load_assets_from_modules([assets])

resources = {
    "base_io_manager": FilesystemIOManager(base_dir="../data"),
    "warsaw_api": WarsawApiResource(api_key=EnvVar("WARSAW_API_KEY")),
}

defs = Definitions(
    assets=all_assets,
    resources=resources,
)

resources = {}

from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules

# Importamos nuestro módulo local
import checks

# El objeto Definitions orquesta todo leyendo automáticamente el módulo importado
defs = Definitions(
    assets=load_assets_from_modules([checks]),
    asset_checks=load_asset_checks_from_modules([checks])
)
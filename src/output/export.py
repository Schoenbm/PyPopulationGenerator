import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)

# Columns kept in the lightweight export
_LIGHT_COLS = ["ID", "geometry", "population_allouee"]

# Columns dropped from the complete export (internal pipeline artifacts)
_DROP_COLS = ["cell_idx"]


def export_results(result: gpd.GeoDataFrame, output_dir: str | Path) -> None:
    """Export allocation results to GeoJSON and CSV files.

    Produces four files in output_dir:
    - buildings_light.geojson  : ID + geometry + population_allouee
    - buildings_light.csv      : ID + population_allouee (no geometry)
    - buildings_full.geojson   : all columns
    - buildings_full.csv       : all columns except geometry

    Args:
        result: GeoDataFrame with population_allouee column.
        output_dir: Directory where output files are written (created if absent).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Lightweight ---
    light = result[_LIGHT_COLS].copy()
    _write_geojson(light, output_dir / "buildings_light.geojson")
    _write_csv(light.drop(columns=["geometry"]), output_dir / "buildings_light.csv")

    # --- Complete ---
    full = result.drop(columns=[c for c in _DROP_COLS if c in result.columns])
    full = _sanitize_for_export(full)
    _write_geojson(full, output_dir / "buildings_full.geojson")
    _write_csv(full.drop(columns=["geometry"]), output_dir / "buildings_full.csv")

    logger.info(
        "Export terminé dans %s  (light: %d lignes, full: %d colonnes)",
        output_dir,
        len(light),
        len(full.columns),
    )


def _write_geojson(gdf: gpd.GeoDataFrame, path: Path) -> None:
    gdf_wgs84 = gdf.to_crs(epsg=4326)
    gdf_wgs84.to_file(path, driver="GeoJSON")
    logger.debug("GeoJSON écrit : %s", path)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    logger.debug("CSV écrit : %s", path)


def _sanitize_for_export(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Convert problematic dtypes (datetime, object with mixed types) for GeoJSON."""
    result = gdf.copy()
    for col in result.columns:
        if col == "geometry":
            continue
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = result[col].astype(str)
    return result

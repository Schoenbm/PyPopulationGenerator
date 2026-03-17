import logging

import geopandas as gpd
import pandas as pd

logger = logging.getLogger(__name__)


def allocate_population(joined: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Distribute INSEE population to residential buildings proportionally to NB_LOGTS.

    For each INSEE grid cell, population (Ind_total) is split across buildings
    proportionally to their NB_LOGTS. Rounding residuals are assigned to the
    building with the largest NB_LOGTS.

    Buildings outside any grid cell (Ind_total = NaN) receive population = 0.

    Args:
        joined: GeoDataFrame from spatial_join, with columns NB_LOGTS, Ind_total,
                and cell_idx (INSEE grid cell index).

    Returns:
        Same GeoDataFrame with an additional integer column `population_allouee`.
    """
    result = joined.copy()
    result["population_allouee"] = 0

    in_grid = result[result["Ind_total"].notna()].copy()

    if in_grid.empty:
        logger.warning("Aucun bâtiment dans un carreau INSEE — population = 0 partout")
        return result

    allocated = _allocate_by_cell(in_grid)
    result.loc[allocated.index, "population_allouee"] = allocated

    total_allocated = result["population_allouee"].sum()
    total_insee = round(in_grid.groupby("cell_idx")["Ind_total"].first().sum())
    logger.info(
        "Population totale allouée : %d  |  Population INSEE totale : %d",
        total_allocated,
        total_insee,
    )

    n_outside = result["Ind_total"].isna().sum()
    if n_outside > 0:
        logger.info("%d batiments hors carreau -> population = 0", n_outside)

    return result


def _allocate_by_cell(in_grid: gpd.GeoDataFrame) -> pd.Series:
    """Allocate population within each grid cell. Returns a Series of integers."""
    result = pd.Series(0, index=in_grid.index, dtype=int)

    for cell_idx, group in in_grid.groupby("cell_idx", sort=False):
        pop = round(group["Ind_total"].iloc[0])

        # Short-circuit: single building gets all population
        if len(group) == 1:
            result.loc[group.index[0]] = pop
            continue

        total_logts = group["NB_LOGTS"].sum()

        if total_logts == 0:
            # No housing units info — spread equally, residual on first row
            per_building = pop // len(group)
            remainder = pop - per_building * len(group)
            result.loc[group.index] = per_building
            result.loc[group.index[0]] += remainder
            continue

        # Proportional allocation with integer rounding
        raw = group["NB_LOGTS"] / total_logts * pop
        rounded = raw.round().astype(int)

        # Adjust residual on the building with the largest NB_LOGTS
        residual = pop - rounded.sum()
        if residual != 0:
            largest_idx = group["NB_LOGTS"].idxmax()
            rounded.loc[largest_idx] += residual

        result.loc[group.index] = rounded

    return result

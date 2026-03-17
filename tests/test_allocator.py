"""Tests for matching/allocator.py — population allocation logic."""

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Polygon

from src.matching.allocator import allocate_population


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_gdf(rows: list[dict]) -> gpd.GeoDataFrame:
    """Build a minimal GeoDataFrame with the columns allocator expects."""
    unit_square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    records = []
    for i, row in enumerate(rows):
        records.append({
            "ID": row.get("ID", f"BAT{i}"),
            "NB_LOGTS": row["NB_LOGTS"],
            "Ind_total": row.get("Ind_total", None),
            "cell_idx": row.get("cell_idx", None),
            "geometry": unit_square,
        })
    return gpd.GeoDataFrame(records, crs="EPSG:2154")


# ── Conservation ──────────────────────────────────────────────────────────────

def test_conservation_exact():
    """Sum of allocated population equals Ind_total for each cell."""
    gdf = _make_gdf([
        {"NB_LOGTS": 10, "Ind_total": 100.0, "cell_idx": 1},
        {"NB_LOGTS": 20, "Ind_total": 100.0, "cell_idx": 1},
        {"NB_LOGTS": 30, "Ind_total": 100.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].sum() == 100


def test_conservation_multiple_cells():
    """Conservation holds independently for each cell."""
    gdf = _make_gdf([
        {"NB_LOGTS": 3, "Ind_total": 10.0, "cell_idx": 1},
        {"NB_LOGTS": 7, "Ind_total": 10.0, "cell_idx": 1},
        {"NB_LOGTS": 5, "Ind_total": 50.0, "cell_idx": 2},
        {"NB_LOGTS": 5, "Ind_total": 50.0, "cell_idx": 2},
    ])
    result = allocate_population(gdf)
    cell1 = result[result["cell_idx"] == 1]["population_allouee"].sum()
    cell2 = result[result["cell_idx"] == 2]["population_allouee"].sum()
    assert cell1 == 10
    assert cell2 == 50


# ── Proportionality ───────────────────────────────────────────────────────────

def test_proportional_allocation():
    """Buildings with equal NB_LOGTS get equal population."""
    gdf = _make_gdf([
        {"NB_LOGTS": 10, "Ind_total": 60.0, "cell_idx": 1},
        {"NB_LOGTS": 10, "Ind_total": 60.0, "cell_idx": 1},
        {"NB_LOGTS": 10, "Ind_total": 60.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert list(result["population_allouee"]) == [20, 20, 20]


def test_proportional_allocation_unequal():
    """Building with double NB_LOGTS gets double population."""
    gdf = _make_gdf([
        {"NB_LOGTS": 10, "Ind_total": 30.0, "cell_idx": 1},
        {"NB_LOGTS": 20, "Ind_total": 30.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    pops = list(result["population_allouee"])
    assert pops[1] == 2 * pops[0]
    assert sum(pops) == 30


# ── Residual adjustment ───────────────────────────────────────────────────────

def test_residual_goes_to_largest():
    """Rounding residual is assigned to building with largest NB_LOGTS."""
    # 3 equal buildings, Ind=10 → each gets 3.33, rounded to 3 → sum=9, residual=+1
    gdf = _make_gdf([
        {"ID": "A", "NB_LOGTS": 5, "Ind_total": 10.0, "cell_idx": 1},
        {"ID": "B", "NB_LOGTS": 10, "Ind_total": 10.0, "cell_idx": 1},  # largest
        {"ID": "C", "NB_LOGTS": 5, "Ind_total": 10.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].sum() == 10
    # Building B (largest NB_LOGTS) absorbs the residual
    pop_b = result.loc[result["ID"] == "B", "population_allouee"].iloc[0]
    pop_a = result.loc[result["ID"] == "A", "population_allouee"].iloc[0]
    pop_c = result.loc[result["ID"] == "C", "population_allouee"].iloc[0]
    assert pop_b >= pop_a and pop_b >= pop_c


def test_residual_negative():
    """Negative residual (over-count after rounding) is also handled."""
    # Designed so rounding gives sum > Ind_total
    gdf = _make_gdf([
        {"ID": "A", "NB_LOGTS": 1, "Ind_total": 2.0, "cell_idx": 1},
        {"ID": "B", "NB_LOGTS": 1, "Ind_total": 2.0, "cell_idx": 1},
        {"ID": "C", "NB_LOGTS": 1, "Ind_total": 2.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].sum() == 2


# ── Single building ───────────────────────────────────────────────────────────

def test_single_building_gets_all():
    """A cell with one building receives 100% of population."""
    gdf = _make_gdf([
        {"NB_LOGTS": 5, "Ind_total": 42.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].iloc[0] == 42


# ── Buildings outside grid ────────────────────────────────────────────────────

def test_outside_grid_gets_zero():
    """Buildings with Ind_total = NaN receive population = 0."""
    gdf = _make_gdf([
        {"NB_LOGTS": 3, "Ind_total": None, "cell_idx": None},
        {"NB_LOGTS": 5, "Ind_total": None, "cell_idx": None},
    ])
    result = allocate_population(gdf)
    assert (result["population_allouee"] == 0).all()


def test_mixed_inside_outside():
    """Inside-grid buildings are allocated normally; outside-grid get zero."""
    gdf = _make_gdf([
        {"NB_LOGTS": 10, "Ind_total": 20.0, "cell_idx": 1},
        {"NB_LOGTS": 10, "Ind_total": 20.0, "cell_idx": 1},
        {"NB_LOGTS": 5,  "Ind_total": None, "cell_idx": None},
    ])
    result = allocate_population(gdf)
    assert result.iloc[2]["population_allouee"] == 0
    assert result.iloc[0]["population_allouee"] + result.iloc[1]["population_allouee"] == 20


# ── Half-integer Ind_total (INSEE noise) ──────────────────────────────────────

def test_half_integer_ind_total():
    """Ind_total = 10.5 is rounded to 11 before allocation, sum is conserved."""
    gdf = _make_gdf([
        {"NB_LOGTS": 1, "Ind_total": 10.5, "cell_idx": 1},
        {"NB_LOGTS": 1, "Ind_total": 10.5, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].sum() == round(10.5)


# ── Zero NB_LOGTS ─────────────────────────────────────────────────────────────

def test_zero_nb_logts_equal_split():
    """When all NB_LOGTS are 0, population is split equally."""
    gdf = _make_gdf([
        {"NB_LOGTS": 0, "Ind_total": 9.0, "cell_idx": 1},
        {"NB_LOGTS": 0, "Ind_total": 9.0, "cell_idx": 1},
        {"NB_LOGTS": 0, "Ind_total": 9.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert result["population_allouee"].sum() == 9
    assert list(result["population_allouee"]) == [3, 3, 3]


# ── Output dtype ─────────────────────────────────────────────────────────────

def test_output_dtype_is_integer():
    """population_allouee column must be integer dtype."""
    gdf = _make_gdf([
        {"NB_LOGTS": 2, "Ind_total": 10.0, "cell_idx": 1},
    ])
    result = allocate_population(gdf)
    assert pd.api.types.is_integer_dtype(result["population_allouee"])

"""Comparison between Filosofi-based and IRIS-based population allocations."""

import logging
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.cm as mcm
import pandas as pd
import contextily as ctx

logger = logging.getLogger(__name__)

_OUTPUT_CSV = "comparison.csv"
_OUTPUT_MAP = "comparison_map.png"
_DPI = 150


def compare_results(
    result_filosofi: gpd.GeoDataFrame,
    result_iris: gpd.GeoDataFrame,
    output_dir: str | Path,
) -> Path:
    """Compare two population allocation results and export CSV + map.

    Merges on building ID, computes difference (IRIS - Filosofi), and produces:
    - comparison.csv : building-level comparison table
    - comparison_map.png : choropleth of the absolute difference

    Args:
        result_filosofi: GeoDataFrame from Filosofi pipeline (population_allouee).
        result_iris:     GeoDataFrame from IRIS pipeline (population_allouee).
        output_dir:      Directory where outputs are written.

    Returns:
        Path to the comparison CSV.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # ── Merge on building ID ──────────────────────────────────────────────────
    left = result_filosofi[["ID", "geometry", "population_allouee"]].rename(
        columns={"population_allouee": "pop_filosofi"}
    )
    right = result_iris[["ID", "population_allouee"]].rename(
        columns={"population_allouee": "pop_iris"}
    )
    merged = left.merge(right, on="ID", how="inner")
    merged["diff"] = merged["pop_iris"] - merged["pop_filosofi"]
    merged["diff_abs"] = merged["diff"].abs()

    # ── Summary stats ─────────────────────────────────────────────────────────
    n = len(merged)
    mae = merged["diff_abs"].mean()
    corr = merged[["pop_filosofi", "pop_iris"]].corr().iloc[0, 1]
    same = (merged["diff"] == 0).sum()

    logger.info("=== COMPARAISON FILOSOFI vs IRIS ===")
    logger.info("Bâtiments comparés       : %d", n)
    logger.info("Identiques               : %d (%.1f%%)", same, same / n * 100)
    logger.info("Erreur absolue moyenne   : %.2f hab/bâtiment", mae)
    logger.info("Corrélation              : %.4f", corr)
    logger.info(
        "Différence totale nette  : %+.0f hab (IRIS - Filosofi)",
        merged["diff"].sum(),
    )
    logger.info(
        "Top 5 écarts absolus :\n%s",
        merged.nlargest(5, "diff_abs")[["ID", "pop_filosofi", "pop_iris", "diff"]]
        .to_string(index=False),
    )

    # ── CSV export ────────────────────────────────────────────────────────────
    csv_path = output_dir / _OUTPUT_CSV
    merged.drop(columns=["geometry"]).to_csv(csv_path, index=False)
    logger.info("CSV comparaison : %s", csv_path)

    # ── Map ───────────────────────────────────────────────────────────────────
    _make_diff_map(merged, output_dir / _OUTPUT_MAP)

    return csv_path


def _make_diff_map(merged: gpd.GeoDataFrame, out_path: Path) -> None:
    """Choropleth map of (pop_iris - pop_filosofi) per building."""
    gdf = merged.to_crs(epsg=3857)

    # Diverging colormap centred on 0
    vmax = max(1, int(gdf["diff"].abs().quantile(0.99)))
    cmap = plt.get_cmap("RdBu_r")
    norm = mcolors.TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)

    fig, axes = plt.subplots(1, 2, figsize=(20, 10))

    for ax, col, title, cmap_local, norm_local in [
        (
            axes[0],
            "pop_filosofi",
            "Filosofi 2019 (carreaux 200m)",
            mcolors.LinearSegmentedColormap.from_list(
                "pop", ["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"]
            ),
            mcolors.Normalize(vmin=0, vmax=int(gdf["pop_filosofi"].quantile(0.99))),
        ),
        (
            axes[1],
            "pop_iris",
            "INSEE RP 2022 (IRIS)",
            mcolors.LinearSegmentedColormap.from_list(
                "pop", ["#ffffb2", "#fecc5c", "#fd8d3c", "#f03b20", "#bd0026"]
            ),
            mcolors.Normalize(vmin=0, vmax=int(gdf["pop_iris"].quantile(0.99))),
        ),
    ]:
        zero = gdf[gdf[col] == 0]
        nonzero = gdf[gdf[col] > 0].copy()

        if not zero.empty:
            zero.plot(ax=ax, color="#cccccc", edgecolor="#999999", linewidth=0.2)
        if not nonzero.empty:
            nonzero["_color"] = nonzero[col].clip(
                upper=int(gdf[col].quantile(0.99))
            ).apply(lambda v: mcolors.to_hex(cmap_local(norm_local(v))))
            nonzero.plot(ax=ax, color=nonzero["_color"].tolist(),
                         edgecolor="#555555", linewidth=0.2)

        ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, zoom="auto")
        sm = mcm.ScalarMappable(cmap=cmap_local, norm=norm_local)
        sm.set_array([])
        fig.colorbar(sm, ax=ax, fraction=0.025, pad=0.02).set_label(
            "Population estimée", fontsize=10
        )
        ax.set_axis_off()
        ax.set_title(title, fontsize=12, pad=8)

    fig.suptitle(
        "Comparaison des allocations de population — Métropole grenobloise",
        fontsize=14,
        y=1.01,
    )
    fig.savefig(out_path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info("Carte comparaison : %s", out_path)

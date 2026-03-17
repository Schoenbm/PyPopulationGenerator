import logging
from pathlib import Path

import numpy as np
import geopandas as gpd

logger = logging.getLogger(__name__)

SURFACE_MOY_DEFAULT: float = 65.0  # m² par logement
HAUTEUR_PAR_ETAGE: float = 3.0     # m par étage (fallback)


def _fix_encoding(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Corrige les colonnes texte doublement encodées (UTF-8 interprété en Latin-1)."""
    for col in gdf.select_dtypes(include="object").columns:
        if col == "geometry":
            continue
        try:
            gdf[col] = gdf[col].apply(
                lambda v: v.encode("latin-1").decode("utf-8") if isinstance(v, str) else v
            )
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass  # colonne déjà correctement encodée
    return gdf


def load_buildings(path: str | Path) -> gpd.GeoDataFrame:
    """Charge le shapefile bâtiments, filtre les résidentiels et estime NB_LOGTS."""
    path = Path(path)
    logger.info("Chargement des bâtiments depuis %s", path)

    gdf = gpd.read_file(path)
    gdf = _fix_encoding(gdf)
    logger.info("CRS : %s — %d bâtiments au total", gdf.crs, len(gdf))

    gdf = filter_residential(gdf)
    logger.info("%d bâtiments résidentiels conservés", len(gdf))

    gdf = estimate_nb_logts(gdf)
    return gdf


def filter_residential(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Garde les bâtiments résidentiels selon USAGE1/USAGE2 et NB_LOGTS."""
    usage1 = gdf.get("USAGE1", gpd.pd.Series(dtype=str))
    usage2 = gdf.get("USAGE2", gpd.pd.Series(dtype=str))
    nb_logts = gdf.get("NB_LOGTS", gpd.pd.Series(dtype=float))

    usage1_null = usage1.isna() | (usage1.astype(str).str.strip() == "")

    mask = (
        (usage1 == "Résidentiel")
        | (usage1_null & (usage2 == "Résidentiel"))
        | ((usage1 == "Indifférencié") & (nb_logts.fillna(0) > 0))
    )

    return gdf[mask].copy()


def estimate_nb_logts(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Estime NB_LOGTS pour les bâtiments où la valeur est manquante ou nulle."""
    if "NB_LOGTS" not in gdf.columns:
        gdf["NB_LOGTS"] = np.nan

    needs_estimate = gdf["NB_LOGTS"].isna() | (gdf["NB_LOGTS"] == 0)
    n_to_estimate = needs_estimate.sum()

    if n_to_estimate == 0:
        logger.info("Aucune estimation NB_LOGTS nécessaire")
        gdf["NB_LOGTS"] = gdf["NB_LOGTS"].astype(int)
        return gdf

    # Calculer nb_étages
    if "NB_ETAGES" in gdf.columns:
        nb_etages = gdf["NB_ETAGES"].fillna(0).clip(lower=0)
        # Pour les lignes à 0, fallback sur HAUTEUR si disponible
        if "HAUTEUR" in gdf.columns:
            from_hauteur = (gdf["HAUTEUR"].fillna(3.0) / HAUTEUR_PAR_ETAGE).round().clip(lower=1)
            nb_etages = nb_etages.where(nb_etages >= 1, from_hauteur)
        nb_etages = nb_etages.clip(lower=1)
    elif "HAUTEUR" in gdf.columns:
        nb_etages = (gdf["HAUTEUR"].fillna(3.0) / HAUTEUR_PAR_ETAGE).round().clip(lower=1)
    else:
        nb_etages = gpd.pd.Series(1.0, index=gdf.index)

    area = gdf.geometry.area
    estimated = np.floor(area * nb_etages / SURFACE_MOY_DEFAULT).clip(lower=1)

    gdf.loc[needs_estimate, "NB_LOGTS"] = estimated[needs_estimate]
    logger.info("%d valeurs NB_LOGTS estimées (surface × étages / %.0f m²)", n_to_estimate, SURFACE_MOY_DEFAULT)

    gdf["NB_LOGTS"] = gdf["NB_LOGTS"].astype(int)
    return gdf

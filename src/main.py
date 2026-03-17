"""Pipeline CLI — distribue la population INSEE aux bâtiments résidentiels.

Usage:
    python src/main.py --step all
    python src/main.py --step load
    python src/main.py --step match
    python src/main.py --step export
    python src/main.py --step visualize
    python src/main.py --step all --verbose
"""

import argparse
import sys
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"

BUILDINGS_SHP = DATA_DIR / "batim_metro_grenoble.shp"
INSEE_SHP = DATA_DIR / "insee_metro_grenoble.shp"

# Intermediate files (GeoPackage — handles all dtypes, fast I/O)
BUILDINGS_GPKG = PROCESSED_DIR / "buildings.gpkg"
INSEE_GPKG = PROCESSED_DIR / "insee.gpkg"
RESULT_GPKG = PROCESSED_DIR / "result.gpkg"


def _setup_logging(verbose: bool) -> None:
    from src.utils.logging_config import setup_logging
    setup_logging(verbose)


# ── Steps ─────────────────────────────────────────────────────────────────────

def step_load(verbose: bool = False) -> None:
    """Load + filter buildings and INSEE grid, save intermediates."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    from src.loaders.buildings import load_buildings
    from src.loaders.insee import load_insee

    log.info("=== STEP load ===")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Chargement des bâtiments : %s", BUILDINGS_SHP)
    buildings = load_buildings(BUILDINGS_SHP)
    buildings.to_file(BUILDINGS_GPKG, driver="GPKG")
    log.info("%d batiments residentiels sauvegardes -> %s", len(buildings), BUILDINGS_GPKG)

    log.info("Chargement des carreaux INSEE : %s", INSEE_SHP)
    insee = load_insee(INSEE_SHP)
    insee.to_file(INSEE_GPKG, driver="GPKG")
    log.info("%d carreaux INSEE sauvegardes -> %s", len(insee), INSEE_GPKG)


def step_match(verbose: bool = False) -> None:
    """Spatial join + population allocation, save result."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.matching.spatial_join import join_buildings_to_insee
    from src.matching.allocator import allocate_population

    log.info("=== STEP match ===")
    _require(BUILDINGS_GPKG, "load")
    _require(INSEE_GPKG, "load")

    buildings = gpd.read_file(BUILDINGS_GPKG)
    insee = gpd.read_file(INSEE_GPKG)

    joined = join_buildings_to_insee(buildings, insee)
    result = allocate_population(joined)

    result.to_file(RESULT_GPKG, driver="GPKG")
    log.info("Resultat sauvegarde -> %s", RESULT_GPKG)


def step_export(verbose: bool = False) -> None:
    """Export GeoJSON and CSV files."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.output.export import export_results

    log.info("=== STEP export ===")
    _require(RESULT_GPKG, "match")

    result = gpd.read_file(RESULT_GPKG)
    export_results(result, PROCESSED_DIR)


def step_visualize(verbose: bool = False) -> None:
    """Generate Folium map."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.output.visualize import make_map

    log.info("=== STEP visualize ===")
    _require(RESULT_GPKG, "match")

    result = gpd.read_file(RESULT_GPKG)
    out = make_map(result, PROCESSED_DIR)
    log.info("Carte disponible : %s", out)


def step_all(verbose: bool = False) -> None:
    """Run the full pipeline end-to-end."""
    import logging
    _setup_logging(verbose)
    logging.getLogger(__name__).info("=== PIPELINE COMPLET ===")

    step_load(verbose)
    step_match(verbose)
    step_export(verbose)
    step_visualize(verbose)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _require(path: Path, prerequisite_step: str) -> None:
    if not path.exists():
        print(
            f"[ERREUR] Fichier intermédiaire manquant : {path}\n"
            f"         Lancez d'abord : python src/main.py --step {prerequisite_step}",
            file=sys.stderr,
        )
        sys.exit(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

_STEPS = {
    "load": step_load,
    "match": step_match,
    "export": step_export,
    "visualize": step_visualize,
    "all": step_all,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Distribue la population INSEE aux bâtiments résidentiels grenoblois."
    )
    parser.add_argument(
        "--step",
        choices=list(_STEPS),
        required=True,
        help="Étape du pipeline à exécuter.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Active les logs DEBUG.",
    )
    args = parser.parse_args()
    _STEPS[args.step](verbose=args.verbose)


if __name__ == "__main__":
    main()

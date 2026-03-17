"""Pipeline CLI — distribue la population INSEE aux bâtiments résidentiels.

Usage:
    python src/main.py --step all                        # pipeline complet (Filosofi)
    python src/main.py --step all --source iris          # pipeline complet (IRIS 2022)
    python src/main.py --step load                       # chargement + filtre
    python src/main.py --step match                      # jointure + allocation
    python src/main.py --step export                     # GeoJSON/CSV
    python src/main.py --step visualize                  # carte PNG
    python src/main.py --step compare                    # comparaison Filosofi vs IRIS
    python src/main.py --step all --verbose              # logs détaillés
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

# Fichiers intermédiaires — communs aux deux sources
BUILDINGS_GPKG = PROCESSED_DIR / "buildings.gpkg"

# Fichiers intermédiaires — spécifiques à chaque source
INSEE_GPKG = PROCESSED_DIR / "insee.gpkg"
IRIS_GPKG = PROCESSED_DIR / "iris.gpkg"

# Résultats finaux
RESULT_FILOSOFI_GPKG = PROCESSED_DIR / "result_filosofi.gpkg"
RESULT_IRIS_GPKG = PROCESSED_DIR / "result_iris.gpkg"


def _setup_logging(verbose: bool) -> None:
    from src.utils.logging_config import setup_logging
    setup_logging(verbose)


def _source_paths(source: str) -> tuple[Path, Path]:
    """Return (grid_gpkg, result_gpkg) for the given source."""
    if source == "iris":
        return IRIS_GPKG, RESULT_IRIS_GPKG
    return INSEE_GPKG, RESULT_FILOSOFI_GPKG


# ── Steps ─────────────────────────────────────────────────────────────────────

def step_load(verbose: bool = False, source: str = "filosofi") -> None:
    """Load + filter buildings and population grid, save intermediates."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    from src.loaders.buildings import load_buildings
    log.info("=== STEP load (source=%s) ===", source)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Bâtiments — communs aux deux sources
    if not BUILDINGS_GPKG.exists():
        log.info("Chargement des bâtiments : %s", BUILDINGS_SHP)
        buildings = load_buildings(BUILDINGS_SHP)
        buildings.to_file(BUILDINGS_GPKG, driver="GPKG")
        log.info("%d bâtiments résidentiels sauvegardés -> %s", len(buildings), BUILDINGS_GPKG)
    else:
        log.info("Bâtiments déjà chargés : %s", BUILDINGS_GPKG)

    # Grille de population
    grid_gpkg, _ = _source_paths(source)

    if source == "iris":
        from src.loaders.iris import load_iris
        log.info("Chargement des IRIS INSEE 2022 (téléchargement auto si nécessaire)…")
        grid = load_iris()
    else:
        from src.loaders.insee import load_insee
        log.info("Chargement des carreaux INSEE Filosofi : %s", INSEE_SHP)
        grid = load_insee(INSEE_SHP)

    grid.to_file(grid_gpkg, driver="GPKG")
    log.info("%d entités sauvegardées -> %s", len(grid), grid_gpkg)


def step_match(verbose: bool = False, source: str = "filosofi") -> None:
    """Spatial join + population allocation, save result."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.matching.spatial_join import join_buildings_to_insee
    from src.matching.allocator import allocate_population

    log.info("=== STEP match (source=%s) ===", source)
    grid_gpkg, result_gpkg = _source_paths(source)

    _require(BUILDINGS_GPKG, "load")
    _require(grid_gpkg, f"load --source {source}")

    buildings = gpd.read_file(BUILDINGS_GPKG)
    grid = gpd.read_file(grid_gpkg)

    joined = join_buildings_to_insee(buildings, grid)
    result = allocate_population(joined)

    result.to_file(result_gpkg, driver="GPKG")
    log.info("Résultat sauvegardé -> %s", result_gpkg)


def step_export(verbose: bool = False, source: str = "filosofi") -> None:
    """Export GeoJSON and CSV files."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.output.export import export_results

    log.info("=== STEP export (source=%s) ===", source)
    _, result_gpkg = _source_paths(source)
    _require(result_gpkg, f"match --source {source}")

    result = gpd.read_file(result_gpkg)
    out_dir = PROCESSED_DIR / source
    export_results(result, out_dir)


def step_visualize(verbose: bool = False, source: str = "filosofi") -> None:
    """Generate static PNG map."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.output.visualize import make_map

    log.info("=== STEP visualize (source=%s) ===", source)
    _, result_gpkg = _source_paths(source)
    _require(result_gpkg, f"match --source {source}")

    result = gpd.read_file(result_gpkg)
    out = make_map(result, PROCESSED_DIR / source)
    log.info("Carte disponible : %s", out)


def step_compare(verbose: bool = False, source: str = "filosofi") -> None:
    """Validate Filosofi allocation against IRIS 2022 census data."""
    import logging
    _setup_logging(verbose)
    log = logging.getLogger(__name__)

    import geopandas as gpd
    from src.output.compare import compare_results

    log.info("=== STEP compare ===")
    _require(RESULT_FILOSOFI_GPKG, "match --source filosofi")
    _require(IRIS_GPKG, "load --source iris")

    result_filosofi = gpd.read_file(RESULT_FILOSOFI_GPKG)
    iris = gpd.read_file(IRIS_GPKG)

    out = compare_results(result_filosofi, iris, PROCESSED_DIR / "compare")
    log.info("Validation terminée : %s", out)


def step_all(verbose: bool = False, source: str = "filosofi") -> None:
    """Run the full pipeline end-to-end."""
    import logging
    _setup_logging(verbose)
    logging.getLogger(__name__).info("=== PIPELINE COMPLET (source=%s) ===", source)

    step_load(verbose, source)
    step_match(verbose, source)
    step_export(verbose, source)
    step_visualize(verbose, source)


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
    "compare": step_compare,
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
        "--source",
        choices=["filosofi", "iris"],
        default="filosofi",
        help="Source de données de population (default: filosofi).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Active les logs DEBUG.",
    )
    args = parser.parse_args()
    _STEPS[args.step](verbose=args.verbose, source=args.source)


if __name__ == "__main__":
    main()

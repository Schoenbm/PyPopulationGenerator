# PyPopulationGenerator

Distribue la population des carreaux INSEE Filosofi aux bâtiments résidentiels de la métropole grenobloise, proportionnellement au nombre de logements.

---

## Fichiers d'entrée requis

Placer les fichiers suivants dans le dossier `data/` :

| Fichier | Description |
|---|---|
| `batim_metro_grenoble.shp` (+ `.dbf`, `.shx`, `.prj`) | Bâtiments BDTopo — métropole de Grenoble |
| `insee_metro_grenoble.shp` (+ `.dbf`, `.shx`, `.prj`) | Carreaux INSEE Filosofi 200m |

> Ces fichiers ne sont pas versionnés (taille > 100 MB). Les obtenir auprès de l'IGN (BDTopo) et de l'INSEE (Filosofi).

**Champs attendus dans `batim_metro_grenoble.shp` :**
- `USAGE1` / `USAGE2` — type d'usage du bâtiment (filtre : `"Résidentiel"`)
- `NB_LOGTS` — nombre de logements (optionnel, estimé si absent)
- `HAUTEUR` — hauteur du bâtiment (utilisée pour estimer les étages)

**Champs attendus dans `insee_metro_grenoble.shp` :**
- `Ind` — population du carreau

---

## Installation

```bash
pip install -r requirements.txt
```

Dépendances principales : `geopandas`, `pandas`, `numpy`, `shapely`, `pyproj`, `matplotlib`, `contextily`.

---

## Lancement

Toutes les commandes s'exécutent **depuis la racine du projet**.

### Pipeline complet (recommandé)

```bash
python src/main.py --step all
```

### Étapes individuelles

```bash
# 1. Chargement et filtre des données
python src/main.py --step load

# 2. Jointure spatiale + allocation de population
python src/main.py --step match

# 3. Export GeoJSON et CSV
python src/main.py --step export

# 4. Génération de la carte Folium
python src/main.py --step visualize
```

### Option verbose (logs détaillés)

```bash
python src/main.py --step all --verbose
```

---

## Fichiers de sortie

Générés dans `data/processed/` :

| Fichier | Contenu |
|---|---|
| `buildings_light.geojson` | ID + géométrie + population allouée |
| `buildings_light.csv` | ID + population allouée (sans géométrie) |
| `buildings_full.geojson` | Tous les attributs + géométrie |
| `buildings_full.csv` | Tous les attributs (sans géométrie) |
| `map.html` | Carte interactive Folium |

---

## Algorithme

1. **Filtre résidentiel** — seuls les bâtiments avec `USAGE1 = "Résidentiel"` sont conservés.
2. **Estimation NB_LOGTS** — si absent : `floor(surface × nb_étages / surface_moy_logement)` où `nb_étages = max(1, round(hauteur / 3.0))`.
3. **Jointure spatiale** — centroïde de chaque bâtiment associé au carreau INSEE qui le contient.
4. **Allocation proportionnelle** — `pop_bâtiment = round(Ind_carreau × NB_LOGTS_bât / Σ NB_LOGTS_carreau)`, avec ajustement du résidu sur le plus grand bâtiment pour que la somme soit exacte.

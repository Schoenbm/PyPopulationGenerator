# PyPopulationGenerator — CLAUDE.md

## Objectif
Distribuer la population des carreaux INSEE Filosofi aux bâtiments résidentiels
de la métropole grenobloise, de façon proportionnelle au nombre de logements.

---

## Structure du projet

```
grenoble-population/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── .gitignore             # inclure data/processed/ si fichiers volumineux
├── data/
│   ├── batim_metro_grenoble.shp  (+ .dbf .prj .qmd .shx)
│   ├── insee_metro_grenoble.shp  (+ .dbf .prj .qmd .shx)
│   └── processed/                # sorties générées par le pipeline
├── src/
│   ├── main.py                   # point d'entrée CLI avec --step
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── buildings.py          # charge batim + filtre résidentiel + estime NB_LOGTS
│   │   └── insee.py              # charge carreaux + calcule Ind_total
│   ├── matching/
│   │   ├── __init__.py
│   │   ├── spatial_join.py       # jointure centroïdes bâtiments ↔ carreaux
│   │   └── allocator.py          # distribution proportionnelle ménages→logements
│   ├── output/
│   │   ├── __init__.py
│   │   ├── export.py             # GeoJSON + CSV
│   │   └── visualize.py          # carte Folium
│   └── utils/
│       ├── __init__.py
│       └── logging_config.py
└── tests/
    └── test_allocator.py
```

---

## Spécifications métier

### 1. Filtre résidentiel & estimation NB_LOGTS (`loaders/buildings.py`)

- **Champs d'usage** : `USAGE1` et `USAGE2` dans `batim_metro_grenoble.shp`
  - Garder les bâtiments dont `USAGE1 == "Résidentiel"` (ou `USAGE2` si `USAGE1` absent/nul)
- **NB_LOGTS** :
  - Si la colonne `NB_LOGTS` existe et est non nulle → l'utiliser directement
  - Sinon → estimer à partir de la surface au sol, de la hauteur du bâtiment et du
    nombre de ménages restant à placer dans le carreau :
    `NB_LOGTS_estimé = floor(surface × nb_étages / surface_moy_logement)`
    où `nb_étages = max(1, round(hauteur / 3.0))` et
    `surface_moy_logement` est calibrée dynamiquement sur le carreau
    (pop_carreau / logements_connus si disponible, sinon 65 m² par défaut).

### 2. Population INSEE (`loaders/insee.py`)

- **Champ de population** : `Ind` dans `insee_metro_grenoble.shp` → utiliser directement
  - Fallback : si `Ind` manquant, faire la somme des colonnes de population disponibles
- **Exclusion de carreaux** : exclure uniquement les carreaux qui ne contiennent
  **aucun bâtiment résidentiel** après jointure spatiale (pas de filtre sur Ind=0)

### 3. Algorithme d'allocation (`matching/allocator.py`)

**Ordre des opérations :**
1. Estimer `NB_LOGTS` pour tous les bâtiments (cf. §1) **avant** l'allocation
2. Jointure spatiale : centroïde de chaque bâtiment → carreau INSEE contenant

**Distribution :**
- Pour chaque carreau, distribuer `Ind` (population) aux bâtiments proportionnellement
  à leur `NB_LOGTS` :
  `pop_bâtiment = round(Ind_carreau × NB_LOGTS_bât / sum(NB_LOGTS_carreau))`
- Résultat : **entier arrondi** — ajustement du résidu sur le bâtiment le plus grand
  pour que `sum(pop_bâtiments) == Ind_carreau` exactement
- Bâtiments hors carreau (centroïde en dehors de toute maille) → population = 0,
  logguer un avertissement

---

## Pipeline CLI (`src/main.py`)

```
python src/main.py --step all        # pipeline complet
python src/main.py --step load       # chargement + filtre
python src/main.py --step match      # jointure + allocation
python src/main.py --step export     # GeoJSON/CSV
python src/main.py --step visualize  # carte Folium
```

---

## Conventions de code

- Python ≥ 3.10
- Type hints sur toutes les fonctions publiques
- Logging via `utils/logging_config.py` (niveau INFO par défaut, DEBUG avec `--verbose`)
- Pas de notebooks dans le dépôt — tout passe par le CLI

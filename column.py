import geopandas as gpd

# Bâtiments
batim = gpd.read_file("data/batim_metro_grenoble.shp")
print("=== BÂTIMENTS ===")
print("CRS:", batim.crs)
print("Nb lignes:", len(batim))
print("Colonnes:", batim.columns.tolist())
print(batim.head(2))

print()

# INSEE
insee = gpd.read_file("data/insee_metro_grenoble.shp")
print("=== INSEE ===")
print("CRS:", insee.crs)
print("Nb lignes:", len(insee))
print("Colonnes:", insee.columns.tolist())
print(insee.head(2))
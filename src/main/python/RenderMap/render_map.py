import geopandas as gpd
import matplotlib.pyplot as plt
for x in ["us-country.json", "us-states.json", "gz_2010_us_050_00_20m.shp"]:
    gdf = gpd.read_file(x)
    gdf.crs = {'init': 'epsg:4326'}
    gdf = gdf.to_crs(epsg=2163)
    fig, ax = plt.subplots(1, 1)
    if "country" in x:
        gdf.plot(ax=ax, linewidth=0.5)
    else:
        gdf.boundary.plot(ax=ax, linewidth=.5)
    ax.axis('off')
    fig.tight_layout()
    fig.savefig(x.replace(".json", "") + ".pdf")

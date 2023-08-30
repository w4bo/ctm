import geopandas as gpd
import matplotlib.pyplot as plt

for x in ["NILZone.shp", "Municipi.shp"]:  # "us-country.json", "us-states.json", "gz_2010_us_050_00_20m.shp"
    gdf = gpd.read_file(x)
    gdf.crs = {'init': 'epsg:3003'}
    # gdf = gdf.to_crs(epsg=4136)
    fig, ax = plt.subplots(1, 1)
    # if "country" in x:
    gdf.plot(ax=ax, linewidth=1, edgecolor='grey', color='white') #
    # else:
    #    gdf.boundary.plot(ax=ax, linewidth=.5)
    ax.axis('off')
    fig.tight_layout()
    fig.savefig(x.replace(".json", "") + ".pdf")

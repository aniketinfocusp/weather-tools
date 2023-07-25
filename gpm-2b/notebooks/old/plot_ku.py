import h5py
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import ListedColormap, BoundaryNorm


filename = '/home/aniket/aniket-backup/anthromet/imerg-data/2b/2B.GPM.DPRGMI.CORRA2022.20161006-S084921-E102153.014807.V07A.hdf5'


data = h5py.File(filename, 'r')

longs = data['/KuKaGMI/Longitude'][:]
lats = data['/KuKaGMI/Latitude'][:]

# airPressure = data['/KuKaGMI/airPressure'][:,:,0]
# airTemperature = data['/KuKaGMI/airTemperature'][:,:,0]

nearSurfPrecipTotRate = data['/KuKaGMI/nearSurfPrecipTotRate'][:,:]


# custom color map
custom_colors = ['#d2f0fa','#50c8fa','#3d83c6','#081396','#00f913','#00a00f','#00630b','#f9f900','#fed865','#fa9700','#dc5000','#ae0000','#7c0200']
custom_cmap = ListedColormap(custom_colors)

color_levels = [0.01, 0.03, 0.05, 0.1, 0.3, 0.5, 0.7, 0.9, 1, 3, 5, 9, 10] 
norm = BoundaryNorm(color_levels, custom_cmap.N, clip=True)

figsize = (30, 20)

fig, axes = plt.subplots(figsize=figsize)
_map = Basemap(projection='mill', lon_0=0)

_map.drawcoastlines()
_map.drawcountries()
parallels = range(-90, 91, 30)
meridians = range(-180, 181, 30)
_map.drawparallels(parallels, labels=[1,0,0,0], fontsize=12, linewidth=0.5, color='gray', dashes=[1, 5])
_map.drawmeridians(meridians, labels=[0,0,0,1], fontsize=12, linewidth=0.5, color='gray', dashes=[1, 5])

x, y = _map(longs, lats)
im = _map.scatter(x, y, c=nearSurfPrecipTotRate, cmap=custom_cmap, norm=norm, zorder=1, s=1)

cbar = plt.colorbar(im, ax=axes)
cbar.set_label('nearSurfPrecipTotRate mm/hr') 

plt.title("imerg-data/2b/2B.GPM.DPRGMI.CORRA2022.20161006-S084921-E102153.014807.V07A.hdf5")

plt.show()
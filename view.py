import rasterio
from rasterio.plot import show
import sys
 
# total arguments
n = len(sys.argv)

uri = sys.argv[1]

band = 1
if n > 2:
    band = int(sys.argv[2])

img = rasterio.open(uri)
show(img.read(band), title=f"file: {uri} band: {band}")  
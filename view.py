import rasterio
from rasterio.plot import show
import sys
 
# total arguments
n = len(sys.argv)
 
uri = sys.argv[1]

img = rasterio.open(uri)
show(img.read(1))  
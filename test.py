import os
import re 
import shutil
import rasterio
from rasterio.plot import show

import xarray as xr
import numpy as np

from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE
from rasterio import MemoryFile

def get_ee_safe_name(uri: str) -> str:
    """Extracts file name and converts it into an EE-safe name"""
    basename = os.path.basename(uri)
    # Strip the extension from the basename.
    basename, _ = os.path.splitext(basename)
    # An asset ID can only contain letters, numbers, hyphens, and underscores.
    # Converting everything else to underscore.
    asset_name = re.sub(r'[^a-zA-Z0-9-_]+', r'_', basename)
    return asset_name


netcdf_multistep = "/home/aniket/infocusp/anthromet/weather-tools-aniket/weather_mv/test_data/test_data_20180101.nc"
grib_single_step = "/home/aniket/infocusp/anthromet/weather-tools-aniket/weather_mv/test_data/test_data_grib_single_timestep"

netcdf_ds = xr.open_dataset(netcdf_multistep)
grib_ds = xr.open_dataset(grib_single_step, engine='cfgrib')


ds = grib_ds
uri = grib_single_step

# Extracting dtype, crs and transform from the dataset & storing them as attributes.
with rasterio.open(uri, 'r') as f:
    dtype, crs, transform = (f.profile.get(key) for key in ['dtype', 'crs', 'transform'])

attrs = ds.attrs

data = list(ds.values())
channel_names = [da.name for da in data]
asset_name = get_ee_safe_name(uri)
output_file_name = f'{asset_name}.tiff'
with MemoryFile() as memfile:
    with memfile.open(driver='COG',
                        dtype=dtype,
                        width=data[0].data.shape[1],
                        height=data[0].data.shape[0],
                        count=len(data),
                        nodata=np.nan,
                        crs=crs,
                        transform=transform,
                        compress='lzw') as f:
        for i, da in enumerate(data):
            f.write(da, i+1)
            # Making the channel name EE-safe before adding it as a band name.
            f.set_band_description(i+1, get_ee_safe_name(channel_names[i]))
            f.update_tags(i+1, band_name=channel_names[i])
            f.update_tags(i+1, **da.attrs)

        # Write attributes as tags in tiff.
        f.update_tags(**attrs)

        target_path = f"/home/aniket/infocusp/anthromet/weather-tools-aniket/{output_file_name}"
        with FileSystems().create(target_path) as dst:
            shutil.copyfileobj(memfile, dst, WRITE_CHUNK_SIZE)

fp = target_path
img = rasterio.open(fp)
print(img)
show(img.read(1))                
print("end")
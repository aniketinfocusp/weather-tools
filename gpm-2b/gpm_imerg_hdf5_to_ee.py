"""Injest GPM Imerg Level 2 dataset into Earth Engine"""
# https://gpm.nasa.gov/data/directory#tab-222-2
import argparse
import time
import psutil
import sys
import h5py
import os
import re
import shutil
import dataclasses
import rasterio
import typing as t
import numpy as np
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE
from weather_mv import loader_pipeline as lp
from pyresample import kd_tree, geometry, create_area_def
from rasterio.transform import from_origin
from rasterio.io import MemoryFile
from weather_mv.loader_pipeline import ee
from weather_mv.loader_pipeline.ee import AssetData
from weather_mv.loader_pipeline.ee import get_ee_safe_name
from weather_mv.loader_pipeline.pipeline import pattern_to_uris
from weather_mv.loader_pipeline.sinks import KwargsFactoryMixin, open_local


@dataclasses.dataclass
class Config:
    """
    Configuration for creating tiff files using HDF5 data
    options:
        selected_paths List[str], None: 
            Specify paths (not just var names) to be included in tiffs. If None, all variables are included.
        seperate_tiff Bool:
            Create seperate tiff files for each variable.
        include_multidim Bool:
            Include multi dimensional variables by flattening them out. For 3 dimensions and above.
    """
    # Options
    # selected_vars = [
    #     "KuKaGMI/surfaceAirPressure",
    #     "KuKaGMI/surfaceAirTemperature",
    #     "KuKaGMI/surfaceVaporDensity"
    # ]
    selected_vars = None
    seperate_tiff = False, #Not implemented
    include_multidim = True

    # Constants
    LONGITUDE_PATH_REGEX = r"[a-zA-Z]*/Longitude"
    LATITUDE_PATH_REGEX = r"[a-zA-Z]*/Latitude"
    MAIN_GROUPS = ["KuGMI", "KuKaGMI"] # Enter paths of all the Groups. Tiffs will be created for each group.
    RESAMPLE_CRS = 4326
    RESAMPLE_RADIUS = 5000
    RESAMPLE_EPSILON = 0
    RESAMPLE_FILL_VALUE = np.nan
    RESAMPLE_RESOLUTION = 0.1
    RESAMPLE_AREA_EXTENT = [-180, -70, 180, 70]
    ORIGIN_LONGITUDE = -180
    ORIGIN_LATITUDE = 70
    # convert to function later
    HEIGHT = 1400   # latitude range * 1/resolution eg (70+70) * 1/0.1 = 1400
    WIDTH = 3600    # same logic as lattiude.
    DTYPE = np.float32
    MULTI_DIM_MAX_COUNT = 8

@dataclasses.dataclass
class ConvertHDF5ToCog(beam.DoFn, KwargsFactoryMixin):
    
    asset_location: str
    ee_asset_type: str = 'IMAGE'

    def get_ee_safe_band_name(self, name):
        """An band name can only contain letters, numbers, hyphens, and underscores."""
        name = re.sub(r'[^a-zA-Z0-9-_]+', r'_', name)
        return name

    def is_h5_file(self, filename):
        """Function to check if given file is a hdf5 file or not."""
        try:
            with h5py.File(filename, 'r'):
                return True
        except OSError:
            return False
        
    def get_all_dataset_from_group(self, group_name, hdf5_file):
        """Given a group name, return all the dataset paths in that group."""
        group = hdf5_file[group_name]
        paths = []
        for item in group.items():
            if(type(item[1])==h5py._hl.dataset.Dataset):
                path = f"{group_name}/{item[0]}"

                if re.match(Config.LATITUDE_PATH_REGEX, path) or re.match(Config.LONGITUDE_PATH_REGEX, path):
                    continue
                
                if Config.selected_vars is not None and path in Config.selected_vars:
                    paths.append(path)

                if Config.selected_vars is None:
                    paths.append(path)
        return paths
    
    def get_hdf_file_attrs(self, hdf_file) -> dict:
        """Convert file level attributes to dict."""
        attributes = hdf_file.attrs.get('FileHeader')
        attributes = attributes.tobytes().decode('utf-8').split(";\n")
        attribute_dict = {}

        for att in attributes:
            if "=" in att:
                key, val = att.split("=")
                attribute_dict[key] = val

        return attribute_dict
    
    def get_hdf_dataset_attrs(self, dataset) -> dict:
        """Get all dataset attributes and convert them to a dict."""
        attr = dataset.attrs
        return {get_ee_safe_name(key):
                    get_ee_safe_name(attr.get(key).decode('utf-8')) 
                    if not isinstance(attr.get(key), np.number) else attr.get(key)
                    for key in attr.keys()}
    
    def count_band_length(self, hdf_file, hdf_paths):
        """Count number of bands to accommodate all variables."""
        count = 0

        for path in hdf_paths:
            dataset = hdf_file[path]

            if dataset.ndim == 2:
                count += 1
            elif dataset.ndim == 3:
                if Config.include_multidim:
                    count += min(dataset.shape[-1], Config.MULTI_DIM_MAX_COUNT)
            else:
                continue
        return count
    
    def get_hdf_dataset(self, hdf_file, hdf_paths) -> t.Iterator[t.Tuple[str, np.ndarray, dict]]:
        """Yields datasets from hdf file for given paths.
        Flatten out multi dimensional datasets if mentioned in Config.
        """
        for path in hdf_paths:
            dataset = hdf_file[path]

            if dataset.ndim == 1:
                continue
            if dataset.ndim == 2:
               yield path, dataset[:], self.get_hdf_dataset_attrs(dataset)
            elif dataset.ndim == 3 and Config.include_multidim:
                # Flatten out 3 Dimensional data
                last_dim = dataset.shape[-1]
                dimension_name = dataset.attrs.get('DimensionNames').decode('utf-8').split(',')[-1]
                attrs = self.get_hdf_dataset_attrs(dataset)
                count = 0
                
                for i in range(last_dim):
                    if count < Config.MULTI_DIM_MAX_COUNT:
                        new_var_name = f"{path}_{dimension_name}_{i}"
                        yield new_var_name, dataset[:,:,i], attrs
                        count += 1
                    else:
                        break
            else:
                print(f"Not implemented for datasets of {dataset.ndim} dims.")
                print(f"skipping {path}")
                # raise NotImplementedError(f"Not implemented for datasets of {dataset.ndim} dims.")


    def print_mem(self, line_no = None):
        process = psutil.Process()
        print(f"process size at {line_no} {process.memory_info().rss / (1024*1024)} MB")
        
    def create_asset(self, hdf_file, dataset_paths, asset_name, group_name):
        """Create a tiff file according to dataset vars."""

        # Get a dataset iterator to read dataset without loading all dataset into the memory.
        dataset_itr = self.get_hdf_dataset(hdf_file, dataset_paths)

        
        file_name = f"{asset_name}.tiff"
        target_path = os.path.join(self.asset_location, file_name)

        file_attrs = self.get_hdf_file_attrs(hdf_file)
        band_count = self.count_band_length(hdf_file, dataset_paths)

        longitude_path = f"{group_name}/Longitude"
        latitude_path = f"{group_name}/Latitude"

        crs = rasterio.crs.CRS.from_epsg(Config.RESAMPLE_CRS)
        transform = from_origin(
            Config.ORIGIN_LONGITUDE,
            Config.ORIGIN_LATITUDE,
            Config.RESAMPLE_RESOLUTION,
            Config.RESAMPLE_RESOLUTION)
        
        

        channel_names = []

        with MemoryFile() as memfile:
            with memfile.open(
                driver='COG',
                dtype=Config.DTYPE,
                width=Config.WIDTH,
                height=Config.HEIGHT,
                count=band_count,
                nodata=Config.RESAMPLE_FILL_VALUE,
                crs=crs,
                transform=transform,
                compress='lzw'
            ) as dst:
                for band, (name, data, attrs) in enumerate(dataset_itr):

                    data = data.astype(Config.DTYPE)

                    area_def = create_area_def('world',
                    Config.RESAMPLE_CRS,
                    area_extent=Config.RESAMPLE_AREA_EXTENT,
                    resolution=Config.RESAMPLE_RESOLUTION)
                
                    swath_def = geometry.SwathDefinition(
                    lons=hdf_file[longitude_path][:],
                    lats=hdf_file[latitude_path][:])

                    # Resample data
                    print(f"resampling {name}")
                    data = kd_tree.resample_nearest(swath_def,
                                                    data,
                                                    area_def,
                                                    radius_of_influence=Config.RESAMPLE_RADIUS,
                                                    epsilon=Config.RESAMPLE_EPSILON,
                                                    fill_value=Config.RESAMPLE_FILL_VALUE)
                    
                    dst.write(data, band+1)
                    del data

                    dst.set_band_description(band+1, self.get_ee_safe_band_name(name))
                    dst.update_tags(band+1, band_name=self.get_ee_safe_band_name(name), **attrs)
                    channel_names.append(self.get_ee_safe_band_name(name))
                    completed = (float(band)/float(band_count))*100

                    del swath_def
                    del area_def

                    self.print_mem()
                    print(f"done {format(completed, '.2f')}%")
                
                dst.update_tags(**file_attrs)
            # Copy in-memory tiff to gcs.
            target_path = os.path.join(self.asset_location, file_name)
            with FileSystems().create(target_path) as dst:
                shutil.copyfileobj(memfile, dst, WRITE_CHUNK_SIZE)

        return AssetData(
            name=asset_name,
            target_path=target_path,
            channel_names=channel_names,
            start_time=file_attrs['StartGranuleDateTime'],
            end_time=file_attrs['StopGranuleDateTime'],
            properties=file_attrs
        )
    
    def process(self, uri):
        with open_local(uri) as local_path:
            assert self.is_h5_file(uri),  f'File "{uri}" not in HDF5 format.'
                
            hdf_file = h5py.File(local_path, 'r')

            for group_name in Config.MAIN_GROUPS:
                scan_type_dataset_paths = self.get_all_dataset_from_group(group_name, hdf_file)

                # No dataset paths for this group
                if len(scan_type_dataset_paths) == 0:
                    continue

                asset_name = f"{get_ee_safe_name(uri)}_{group_name}"
                print(f"Creating {asset_name}.")

                yield self.create_asset(hdf_file, scan_type_dataset_paths, asset_name, group_name)

def validate_arguments(known_args: argparse.Namespace):
    """Validates arguments for DWD bufr data pipeline."""
    if known_args.ee_asset_type != "IMAGE":
        raise RuntimeError("--ee_asset_type must be 'IMAGE'.")

if __name__ == "__main__":
    start_time = time.time()

    known_args, pipeline_args = lp.run(sys.argv)
    validate_arguments(known_args)

    all_uris = list(pattern_to_uris(known_args.uris))

    if not all_uris:
        raise FileNotFoundError(
            f"File prefix '{known_args.uris}' matched no objects."
        )
    
    with beam.Pipeline(argv=pipeline_args) as p:
        (
            p
            | "Create" >> beam.Create(all_uris)
            | "FilterFiles" >> ee.FilterFilesTransform.from_kwargs(**vars(known_args))
            | "ReshuffleFiles" >> beam.Reshuffle()
            | "ConvertHDF5ToCog" >> beam.ParDo(ConvertHDF5ToCog.from_kwargs(**vars(known_args)))
            | "IngestIntoEE" >> ee.IngestIntoEETransform.from_kwargs(**vars(known_args))
        )

    print("Pipeline finished.")

    print("--- %s seconds ---" % (time.time() - start_time))

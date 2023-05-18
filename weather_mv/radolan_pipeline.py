import argparse
import wradlib as wrl
import rasterio
from rasterio.io import MemoryFile
import apache_beam as beam
from apache_beam.io.filesystem import CompressionTypes, FileSystem, CompressedFile, DEFAULT_READ_BUFFER_SIZE
from apache_beam.io.filesystems import FileSystems
from weather_mv.loader_pipeline.sinks import ToDataSink, KwargsFactoryMixin
from weather_mv.loader_pipeline.ee import ToEarthEngine
from weather_mv.loader_pipeline.pipeline import pattern_to_uris
import dataclasses
import argparse
import tempfile
import os
import logging
import shutil
import json
import subprocess
import typing as t
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_asset_name(uri):
    return uri.split('/')[-1].split('.')[0]

def _get_uri_extesion(uri):
    return uri.split('.')[-1]

def copy(src: str, dst: str) -> None:
    """Copy data via `gcloud alpha storage` or `gsutil`."""
    errors: t.List[subprocess.CalledProcessError] = []
    for cmd in ['gcloud alpha storage cp', 'gsutil cp']:
        try:
            subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True, text=True, input="n/n")
            return
        except subprocess.CalledProcessError as e:
            errors.append(e)

    msg = f'Failed to copy file {src!r} to {dst!r}'
    err_msgs = ', '.join(map(lambda err: repr(err.stderr.decode('utf-8')), errors))
    logger.error(f'{msg} due to {err_msgs}.')
    raise EnvironmentError(msg, errors)
@dataclasses.dataclass
class FilterFiles(beam.DoFn):

    asset_location: str

    def process(self, uri):
        asset_name = _get_asset_name(uri)

        target_path = os.path.join(self.asset_location, f"{asset_name}.tif")
        
        if FileSystems.exists(target_path):
            logger.info(f"File {asset_name}.tif already exists at out dir ({self.asset_location}).")
            return
        
        yield uri
@dataclasses.dataclass
class ConvertRadolanToTiff(beam.DoFn):

    asset_location: str

    def get_crs(self):
        proj_osr = wrl.georef.create_osr("dwd-radolan")
        
        wkt_str = proj_osr.ExportToWkt()

        return rasterio.crs.CRS.from_wkt(wkt_str)
        
    def get_transform(self, meta):
        nrows = meta['nrow']
        ncols = meta['ncol']

        radolan_grid_xy = wrl.georef.get_radolan_grid(nrows, ncols)
        
        ul = (radolan_grid_xy[-1, 0, :][0], radolan_grid_xy[-1, 0, :][1])
        ur = (radolan_grid_xy[-1, -1, :][0], radolan_grid_xy[-1, -1, :][1])
        lr = (radolan_grid_xy[0, -1, :][0], radolan_grid_xy[0, -1, :][1])
        ll = (radolan_grid_xy[0, 0, :][0], radolan_grid_xy[0, 0, :][1])
        
        return rasterio.transform.from_bounds(ul[0], lr[1], ur[0], ul[1], width=ncols, height=nrows)

    def get_dwd_header(self, uri):
        filehandle = wrl.io.get_radolan_filehandle(uri)
        
        header = wrl.io.read_radolan_header(filehandle)
        
        return wrl.io.parse_dwd_composite_header(header)
                                                 
    def get_dataset(self, uri):
        ds = xr.open_dataset(uri, engine="radolan")

        for data_var_name in list(ds.data_vars):
            flipped = xr.DataArray(np.flipud(ds[data_var_name].values), coords={'y': ds['y'], 'x': ds['x']}, dims=['y','x'])
            ds[data_var_name] = flipped
        
        return ds

    def convert(self, ds, uri):
        crs = self.get_crs()

        meta = self.get_dwd_header(uri)
        
        transform = self.get_transform(meta)

        data = list(ds.values())
        
        asset_name = _get_asset_name(uri)
        output_path = os.path.join(self.asset_location, f"{_get_asset_name(asset_name)}.tif")

        with MemoryFile() as memfile:
            with memfile.open(
                driver='COG',
                dtype='float64',
                width=data[0].data.shape[1],
                height=data[0].data.shape[0],
                count=len(data),
                nodata=np.nan,
                crs=crs,
                transform=transform,
                compress='lzw'
            ) as f:
                for i, da in enumerate(data):
                    f.write(da, i+1)

            with FileSystems().create(output_path) as dst:
                shutil.copyfileobj(memfile, dst)

        return output_path
    
    def process(self, uri):
        ds = self.get_dataset(uri)
        
        yield self.convert(ds, uri)


@dataclasses.dataclass
class RadolanPipeline(ToDataSink):
    uri: str

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--uri', type=str, required=True, default=None,
                               help='URI of file')
    
    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_options: t.List[str]):
        pass



@dataclasses.dataclass
class ReprojectTiffToWSG84(beam.DoFn):

    asset_location: str

    def process(self, uri):
        asset_name = _get_asset_name(uri)

        input_file = uri

        output_file = os.path.join(self.asset_location, f"{asset_name}_reproj.tiff")

        cmd = ["gdalwarp", "-t_srs", "EPSG:4326", input_file, output_file]
        
        subprocess.run(cmd)

        if os.path.exists(input_file):
            os.remove(input_file)

        yield output_file

@dataclasses.dataclass
class MoveRadolanToEE(ToEarthEngine):
    
    asset_location: str

    def expand(self, paths):
        
        out = (
            paths
            | 'Filter Files' >> beam.ParDo(FilterFiles(self.asset_location))
            | beam.Reshuffle()
            | 'Convert' >> beam.ParDo(ConvertRadolanToTiff(self.asset_location))
            | 'Reproject' >> beam.ParDo(ReprojectTiffToWSG84(self.asset_location))
            | 'Log' >> beam.Map(print)
        )

def pipeline(known_args: argparse.Namespace):
    
    all_uris = list(pattern_to_uris(known_args.uris, False))

    asset_location = known_args.asset_location

    print(f"asset loc: {asset_location}")

    with beam.Pipeline() as p:
        paths = p | 'Create' >> beam.Create(all_uris)

        paths | MoveRadolanToEE.from_kwargs(**vars(known_args))

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        prog='radolan-convert',
        description='Weather Mover loads weather data from cloud storage into analytics engines.'
    )

    parser.add_argument('-i', '--uris', type=str, required=True,
                      help="URI glob pattern matching input weather data, e.g. 'gs://ecmwf/era5/era5-2015-*.gb'. Or, "
                           "a path to a Zarr.")
    parser.add_argument('--zarr', action='store_true', default=False,
                      help="Treat the input URI as a Zarr. If the URI ends with '.zarr', this will be set to True. "
                           "Default: off")
    parser.add_argument('--zarr_kwargs', type=json.loads, default='{}',
                      help='Keyword arguments to pass into `xarray.open_zarr()`, as a JSON string. '
                           'Default: `{"chunks": null, "consolidated": true}`.')
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                      help='Preview the weather-mv job. Default: off')
   

    MoveRadolanToEE.add_parser_arguments(parser)

    args = parser.parse_args()
    args.first_uri = "dummy"

    pipeline(args)
    
    print("end pipeline")



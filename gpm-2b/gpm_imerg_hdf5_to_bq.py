import time
import h5py
import sys
import json
import argparse
import dataclasses
import apache_beam as beam
from weather_mv import loader_pipeline as lp
from weather_mv.loader_pipeline.pipeline import pattern_to_uris
from weather_mv.loader_pipeline.sinks import KwargsFactoryMixin, open_local
from weather_mv.loader_pipeline.util import to_json_serializable_type
from apache_beam.io import WriteToBigQuery, BigQueryDisposition


# Constants
NSCANS = 7934
NRAYS = 49
MULTIDIM_MAX = 8

def validate_arguments(known_args: argparse.Namespace):
    """Validates arguments for DWD bufr data pipeline."""
    pass

@dataclasses.dataclass
class ExtractRowFromScan(beam.DoFn, KwargsFactoryMixin):

    def process(self, scan):
        
        for i in range(NRAYS):
            row = {}
            
            for var, data in scan.items():
                var_name = var.split('/')[1]
                row[var_name] = to_json_serializable_type(data[i])

            print(f"Extracted Row: {row}")

            yield row

@dataclasses.dataclass
class PrepareScans(beam.DoFn, KwargsFactoryMixin):

    def get_all_dataset_from_group(self, group_name, hdf5_file):
        """Given a group name, return all the dataset paths in that group."""
        group = hdf5_file[group_name]
        paths = []
        for item in group.items():
            if(type(item[1])==h5py._hl.dataset.Dataset):
                path = f"{group_name}/{item[0]}"
                paths.append(path)

        return paths

    def process(self, uri):
        with open_local(uri) as local_path:

            hdf_file = h5py.File(local_path, 'r')

            for group_name in ['KuGMI']:
                dataset_paths = self.get_all_dataset_from_group(group_name, hdf_file)
                
                for i in range(NSCANS):
                    scan_data = {}
                    for path in dataset_paths:

                        dataset = hdf_file[path]

                        if dataset.ndim == 1:
                            continue
                        elif dataset.ndim == 2:
                            scan_data[path] = dataset[i]

                        elif dataset.ndim == 3:
                            last_dim = dataset.shape[-1]
                            dimension_name = dataset.attrs.get('DimensionNames').decode('utf-8').split(',')[-1]
                            count = 0

                            for j in range(last_dim):
                                if count < MULTIDIM_MAX:
                                    new_var_name = f"{path}_{dimension_name}_{j}"
                                    scan_data[new_var_name] = dataset[i,:,j]
                                    count += 1
                                else:
                                    break

                    yield scan_data


if __name__ == "__main__":
    start_time = time.time()

    known_args, pipeline_args = lp.run(sys.argv)
    validate_arguments(known_args)

    all_uris = list(pattern_to_uris(known_args.uris))

    if not all_uris:
        raise FileNotFoundError(
            f"File prefix '{known_args.uris}' matched no objects."
        )
    
    # create BQ table here
    
    with beam.Pipeline(argv=pipeline_args) as p:
        extracted_rows = (
            p
            | "Create" >> beam.Create(all_uris)
            | "Prepare Scans" >> beam.ParDo(PrepareScans.from_kwargs(**vars(known_args)))
            | "Extract Row" >> beam.ParDo(ExtractRowFromScan.from_kwargs(**vars(known_args)))
        )
        (
            extracted_rows 
            | WriteToBigQuery(
                project="ee-aniket",
                dataset="mydataset",
                table="gpm-3",
                schema="SCHEMA_AUTODETECT",
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
    print("--- %s seconds ---" % (time.time() - start_time))
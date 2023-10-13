import getpass
import logging
import argparse
import itertools
import typing as t
from concurrent.futures import ThreadPoolExecutor
from config_processing.parsers import process_config
from config_processing.partition import PartitionConfig
from config_processing.manifest import FirestoreManifest
from database import FirestoreClient

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.DEBUG)

db_client = FirestoreClient()



def start_manifest_job(config_name: str, offset: int, chunk_size: int, force_download):

    def task(selection: dict):
        logger.info(f"from thread {selection}")
        if partition_obj.new_downloads_only_by_selection(selection):
            partition_obj.update_manifest_selection(selection)
        return selection

    config = None
    manifest = FirestoreManifest()

    logger.info("Downloading config file.")

    with db_client._open_local(config_name) as local_path:
        with open(local_path, "r", encoding="utf-8") as f:
            config = process_config(f, config_name)

    if config is None:
        raise "Error downloading config file."

    logger.info("Downloaded config file.")

    config.force_download = force_download
    config.user_id = getpass.getuser()

    partition_obj = PartitionConfig(config, None, manifest)

    selection_itr = partition_obj.prepare_selections()

    chunked_itr = itertools.islice(selection_itr, offset, offset+chunk_size)

    logger.info("Starting threadpool.")
    with ThreadPoolExecutor(max_workers=32) as executor:
        for index, result in enumerate(executor.map(task, chunked_itr)):
            logger.info(index)
        # for selection in itertools.islice(selection_itr, offset, offset+chunk_size):
        #     logger.info(f"selection {selection}")
        #     executor.submit(task, partition_obj, selection)


if __name__ == "__main__":
    logger.info("start")
    print("start")
    parser = argparse.ArgumentParser(description="Partitioner")

    parser.add_argument("--config_name", type=str, required=True, help="Config file for partitioning.")
    parser.add_argument("--offset", type=int, required=True, help="Offset")
    parser.add_argument("--chunk_size", type=int, required=True, help="Chunk Size")
    parser.add_argument("--force_download", action="store_true", help="Force download all partitions.")


    args = parser.parse_args()

    config_name = args.config_name
    offset = args.offset
    chunk_size = args.chunk_size
    force_download = args.force_download

    logger.info(f"offset {offset}, chunk size {chunk_size}.")
    logger.info(f"Starting partitioning for {config_name}.")
    try:
        start_manifest_job(config_name, offset, chunk_size, force_download)
    except Exception as e:
        logger.error(f"Couldn't finish partitioning for chunk {offset}, {chunk_size}. Error: {e}.")
    logger.info(f"Finished partitioning for {config_name}.")
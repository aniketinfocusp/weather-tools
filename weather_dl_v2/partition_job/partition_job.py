import getpass
import logging
import time
import typing as t
import argparse
import concurrent.futures
from config_processing.parsers import process_config
from config_processing.partition import PartitionConfig
from config_processing.manifest import FirestoreManifest
from database import FirestoreClient
from manifest_job_creator import create_manifest_job, get_manifest_worker_count, get_manifest_worker_count_by_job_id

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.DEBUG)

db_client = FirestoreClient()

DISTRIBUTED_JOB_MAX_WORKERS_HARD_LIMIT = 50

def manifest_job_distributed(partition_obj: PartitionConfig, chunk_size, max_workers, auto_chunking):
    config_name = partition_obj.config.config_name
    number_of_parititons = partition_obj.get_number_of_partition()
    force_download = partition_obj.config.force_download
    max_worker_count = max_workers
    
    logger.info(f"Number of partitions {number_of_parititons}")

    if chunk_size == None:
        raise "Chunk size missing."
    
    if max_workers == None:
        raise "Max workers missing."
    
    # Do automatic chunking if turned on.
    if auto_chunking:
        chunk_size = (int)(number_of_parititons / max_worker_count) + 10 
    
    # List of all spawned manifest job ids.
    job_ids = []

    for offset in range(0, number_of_parititons, chunk_size):
        logger.info(f"Creating manifest job for offset {offset}.")

        while get_manifest_worker_count_by_job_id(job_ids) > max_worker_count:
            logger.info("Max worker count reached. Waiting for workers...")
            time.sleep(60)

        worker_count = get_manifest_worker_count_by_job_id(job_ids)
        logger.info(f"Current worker count: {worker_count}")
        job_id = create_manifest_job(config_name, offset, chunk_size, force_download)
        job_ids.append(job_id)

    # wait for partitioning to finish by looking at the worker count.
    while get_manifest_worker_count_by_job_id(job_ids) > 0:
        logger.info("Waiting for all workers to complete.")
        time.sleep(60)

def manifest_job(partition_obj: PartitionConfig):
    def task(selection: dict):
        #skip partition
        if partition_obj.new_downloads_only_by_selection(selection):
                partition_obj.update_manifest_selection(selection)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for selection in partition_obj.prepare_selections():
            executor.submit(task, selection)


#TODO: remove local_config_path        
def start_processing_config(config_name, licenses, distributed=False, chunk_size=None, max_workers=None, auto_chunking=False, force_download=False, config_path=None):
    config = {}
    manifest = FirestoreManifest()

    logger.info(f"Opening config file {config_name}.")
    if config_path:
        with open(config_path, "r", encoding="utf-8") as f:
                config = process_config(f, config_name)
    else:
        with db_client._open_local(config_name) as local_path:
            with open(local_path, "r", encoding="utf-8") as f:
                config = process_config(f, config_name)

    config.force_download = force_download
    config.user_id = getpass.getuser()

    logger.info(f"Opened config file {config_name}.")

    partition_obj = PartitionConfig(config, None, manifest)

    # Make entry in 'download' & 'queues' collection.
    db_client._start_download(config_name, config.client)
    db_client._mark_partitioning_status(
        config_name, "Partitioning in-progress."
    )

    try:
        # Prepare partitions
        logger.info(f"Preparing partitions for {config_name}.")

        start_time = time.time()

        if distributed and chunk_size:
            logger.info("Using distributed paritioning.")
            manifest_job_distributed(partition_obj, chunk_size, max_workers, auto_chunking)
            logger.info("distributed partitioning took--- %s seconds ---" % (time.time() - start_time))
        else:
            manifest_job(partition_obj)
            logger.info("partitioning took--- %s seconds ---" % (time.time() - start_time))

        

        db_client._mark_partitioning_status(
            config_name, "Partitioning completed."
        )
        db_client._update_queues_on_start_download(config_name, licenses)
        logger.info(f"Partitions for {config_name} prepared and added to queues {licenses}.")
    except Exception as e:
        error_str = f"Partitioning failed for {config_name} due to {e}."
        logger.error(error_str)
        db_client._mark_partitioning_status(config_name, error_str)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Partitioner")

    parser.add_argument("--config_name", type=str, required=True, help="Config file for partitioning.")
    parser.add_argument("--licenses", nargs="+", type=str, required=True, help="List of licenses.")
    parser.add_argument("--distributed", action="store_true", help="Distribute the work load of creating partitions for larger partitions.")
    parser.add_argument("--chunk_size", type=int, help="Chunk size for distributed job.")
    parser.add_argument("--max_workers", type=int, help="Maximum number of workers to use.")
    parser.add_argument("--auto_chunking", action="store_true", help="Automatically chunk the data according to max_workers. Overides chunk size.")
    parser.add_argument("--force_download", action="store_true", help="Force download all partitions.")

    # Args for devlopment
    parser.add_argument("--local_config_path", type=str, help="Config file for partitioning.")

    args = parser.parse_args()

    config_name = args.config_name
    licenses = args.licenses
    distributed = args.distributed
    chunk_size = args.chunk_size
    max_workers = args.max_workers
    auto_chunking = args.auto_chunking
    force_download = args.force_download
    local_config_path = args.local_config_path

    #TODO: set default value from argsparse
    if distributed and chunk_size == None:
        logger.info("Chunk size not mentioned. Using default: 100000.")
        chunk_size = 100000

    if distributed and max_workers == None:
        logger.info("Max workers not mentioned. Using default: 32")
        max_workers = 32

    if max_workers > DISTRIBUTED_JOB_MAX_WORKERS_HARD_LIMIT:
        logger.info(f"Max workers above hard limit. Reducing to {DISTRIBUTED_JOB_MAX_WORKERS_HARD_LIMIT}")
        max_workers = min(max_workers, DISTRIBUTED_JOB_MAX_WORKERS_HARD_LIMIT)

    logger.info(f"Starting partitioning for {config_name}.")
    start_processing_config(config_name, licenses, distributed, chunk_size, max_workers, auto_chunking, force_download, local_config_path)
    logger.info(f"Finished partitioning for {config_name}.")
import logging
import yaml
import uuid
import typing as t
from os import path
from kubernetes import client, config
from server_config import get_config

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger(__name__).setLevel(logging.DEBUG)

def create_partition_job(config_name: str, licenses: t.List[str], distributed=False, chunk_size=None, max_workers=None, auto_chunking=False, force_download=False):
    config.load_config()

    with open(path.join(path.dirname(__file__), "partition_job.yaml")) as f:
        dep = yaml.safe_load(f)
        uid = uuid.uuid4()
        dep["metadata"]["name"] = f"partition-job-id-{uid}"
        dep["spec"]["template"]["metadata"]["labels"]["config"] = config_name
        dep["spec"]["template"]["spec"]["containers"][0]["command"] = [
            "python",
            "partition_job.py",
            "--config_name", config_name,
            "--licenses", ' '.join(licenses)
        ]

        extra = []

        if force_download:
            extra.append("--force_download")
        if distributed:
            extra.append("--distributed")
        if chunk_size:
            extra.append("--chunk_size")
            extra.append(str(chunk_size))
        if max_workers:
            extra.append("--max_workers")
            extra.append(str(max_workers))
        if auto_chunking:
            extra.append("--auto_chunking")

        dep["spec"]["template"]["spec"]["containers"][0]["command"].extend(extra)

        dep["spec"]["template"]["spec"]["containers"][0]["image"] = get_config().partition_job_k8_image
        batch_api = client.BatchV1Api()
        batch_api.create_namespaced_job(body=dep, namespace="default")

        logger.info("Created Partition Job.")
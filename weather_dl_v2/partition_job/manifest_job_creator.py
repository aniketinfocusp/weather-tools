
import yaml
import uuid
import typing as t
from os import path
from kubernetes import client, config
from partition_config import get_config


def get_manifest_worker_count(config_name: str):
    config.load_config()
    v1 = client.CoreV1Api()
    label_selector = f"config={config_name}"
    pods = v1.list_namespaced_pod(namespace="default", label_selector=label_selector)
    return len(pods.items)

def get_manifest_worker_count_by_job_id(job_ids: t.List[str]) -> int:
    config.load_config()
    v1 = client.CoreV1Api()
    label_selector = f"job-name in ({', '.join(job_ids)})"
    pods = v1.list_namespaced_pod(namespace="default", label_selector=label_selector)
    return len(pods.items)

def create_manifest_job(config_name: str, offset: int, chunk_size: int, force_download: bool) -> str:
    config.load_config()
    uid = uuid.uuid4()
    job_id = f"manifest-job-id-{uid}"
    with open(path.join(path.dirname(__file__), "manifest_job.yaml")) as f:
        dep = yaml.safe_load(f)
        dep["metadata"]["name"] = job_id
        # dep["spec"]["template"]["metadata"]["labels"]["config"] = config_name
        dep["spec"]["template"]["spec"]["containers"][0]["command"] = [
            "python",
            "manifest_job.py",
            "--config_name", config_name,
            "--offset", str(offset),
            "--chunk_size", str(chunk_size),
        ]
        if force_download:
            dep["spec"]["template"]["spec"]["containers"][0]["command"].append("--force_download")
        dep["spec"]["template"]["spec"]["containers"][0]["image"] =  get_config().partition_job_k8_image
        batch_api = client.BatchV1Api()
        batch_api.create_namespaced_job(body=dep, namespace="default")

    return job_id
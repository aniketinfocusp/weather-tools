import dataclasses
import typing as t
import json

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class PartitionConfig:
    download_collection: str = ""
    queues_collection: str = ""
    license_collection: str = ""
    manifest_collection: str = ""
    storage_bucket: str = ""
    gcs_project: str = ""
    partition_job_k8_image: str = ""
    kwargs: t.Optional[t.Dict[str, Values]] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_dict(cls, config: t.Dict):
        config_instance = cls()

        for key, value in config.items():
            if hasattr(config_instance, key):
                setattr(config_instance, key, value)
            else:
                config_instance.kwargs[key] = value

        return config_instance


deployment_config = None


def get_config():
    global deployment_config
    partition_config_json = "config/config.json"

    if deployment_config is None:
        with open(partition_config_json) as file:
            config_dict = json.load(file)
            deployment_config = PartitionConfig.from_dict(config_dict)

    return deployment_config

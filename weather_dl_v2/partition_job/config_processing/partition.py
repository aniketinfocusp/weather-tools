import logging
import copy as cp
import dataclasses
import time
import itertools
import typing as t

from config_processing.manifest import Manifest
from config_processing.parsers import prepare_target_name, prepare_target_name_by_selection
from config_processing.config import Config
from config_processing.stores import Store, FSStore

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class PartitionConfig:
    """Partition a config into multiple data requests.

    Partitioning involves four main operations: First, we fan-out shards based on
    partition keys (a cross product of the values). Second, we filter out existing
    downloads (unless we want to force downloads). Last, we assemble each partition
    into a single Config.

    Attributes:
        store: A cloud storage system, used for checking the existence of downloads.
        manifest: A download manifest to register preparation state.
    """

    config: Config
    store: Store
    manifest: Manifest

    def _create_partition_selection(self, option: t.Tuple) -> t.Dict[str, any]:
        copy = cp.deepcopy(self.config.selection)
        for idx, key in enumerate(self.config.partition_keys):
            copy[key] = [option[idx]]
        return copy
    
    def _create_parition_selection_optimized(self, option: t.Tuple) -> t.Dict[str, any]:
        selection = {}
        partition_keys = self.config.partition_keys
        all_keys = self.config.selection.keys()
        for key in all_keys:
            if key not in partition_keys:
                selection[key] = self.config.selection[key]

        for idx, key in enumerate(partition_keys):
            selection[key] = [option[idx]]

        return selection


    def _create_partition_config(self, option: t.Tuple) -> Config:
        copy = cp.deepcopy(self.config.selection)
        out = cp.deepcopy(self.config)
        for idx, key in enumerate(self.config.partition_keys):
            copy[key] = [option[idx]]

        out.selection = copy
        return out

    def skip_partition(self, config: Config) -> bool:
        """Return true if partition should be skipped."""

        if config.force_download:
            return False

        target = prepare_target_name(config)
        if self.store.exists(target):
            logger.info(f"file {target} found, skipping.")
            self.manifest.skip(
                config.config_name,
                config.dataset,
                config.selection,
                target,
                config.user_id,
            )
            return True

        return False
    
    def skip_partition_by_selection(self, selection: dict) -> bool:
        if self.config.force_download:
            return False
        
        target = prepare_target_name_by_selection(selection, self.config.partition_keys, self.config.target_path)
        if self.store.exists(target):
            logger.info(f"file {target} found, skipping.")
            self.manifest.skip(
                self.config.config_name,
                self.config.dataset,
                selection,
                target,
                self.config.user_id
            )
            return True

        return False

    def prepare_selections(self) -> t.Iterator[dict]:
        keys = [self.config.selection[key] for key in self.config.partition_keys]
        itr = itertools.product(*keys)
        for option in itr:
            yield self._create_parition_selection_optimized(option)

    def get_number_of_partition(self) -> int:
        keys = [self.config.selection[key] for key in self.config.partition_keys]
        itr = itertools.product(*keys)
        return len(list(itr))

    def prepare_partitions(self) -> t.Iterator[Config]:
        """Iterate over client parameters, partitioning over `partition_keys`.

        This produces a Cartesian-Cross over the range of keys.

        For example, if the keys were 'year' and 'month', it would produce
        an iterable like:
            ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)

        Returns:
            An iterator of `Config`s.
        """
        for option in itertools.product(
            *[self.config.selection[key] for key in self.config.partition_keys]
        ):
            yield self._create_partition_config(option)

    def new_downloads_only(self, candidate: Config) -> bool:
        """Predicate function to skip already downloaded partitions."""
        if self.store is None:
            self.store = FSStore()
        should_skip = self.skip_partition(candidate)

        return not should_skip
    
    def new_downloads_only_by_selection(self, selection: dict) -> bool:
        if self.store is None:
            self.store = FSStore()
        should_skip = self.skip_partition_by_selection(selection)

        return not should_skip
    
    def update_manifest_selection(self, selection: dict):
        """Updates the DB"""
        location = prepare_target_name_by_selection(selection, self.config.partition_keys, self.config.target_path)
        self.manifest.schedule(
            self.config.config_name,
            self.config.dataset,
            selection,
            location,
            self.config.user_id
        )
        logger.info(f"Created partition {location!r}.")

    def update_manifest_collection(self, partition: Config) -> Config:
        """Updates the DB."""
        location = prepare_target_name(partition)
        self.manifest.schedule(
            partition.config_name,
            partition.dataset,
            partition.selection,
            location,
            partition.user_id,
        )
        logger.info(f"Created partition {location!r}.")


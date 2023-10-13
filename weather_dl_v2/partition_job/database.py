import abc
import time
import logging
import tempfile
import contextlib
import firebase_admin
import typing as t
from firebase_admin import firestore
from firebase_admin import credentials
from gcloud import storage
from google.cloud.firestore_v1 import DocumentSnapshot, DocumentReference
from google.cloud.firestore_v1.types import WriteResult
from google.cloud.firestore_v1.base_query import FieldFilter, And
from partition_config import get_config

logger = logging.getLogger(__name__)

class Database(abc.ABC):

    @abc.abstractmethod
    def _get_db(self):
        pass


class DatabaseOperations(abc.ABC):

    @abc.abstractmethod
    def _start_download(self, config_name: str, client_name: str):
        pass

    @abc.abstractmethod
    def _mark_partitioning_status(self, config_name: str, status: str):
        pass

    @abc.abstractmethod
    def _update_queues_on_start_download(self, config_name: str, licenses: list):
        pass

    @abc.abstractmethod
    def _open_local(self, file_name) -> t.Iterator[str]:
        pass


class FirestoreClient(Database, DatabaseOperations):

    def _get_db(self) -> firestore.firestore.Client:
        """Acquire a firestore client, initializing the firebase app if necessary.
        Will attempt to get the db client five times. If it's still unsuccessful, a
        `ManifestException` will be raised.
        """
        db = None
        attempts = 0

        while db is None:
            try:
                db = firestore.client()
            except ValueError as e:
                # The above call will fail with a value error when the firebase app is not initialized.
                # Initialize the app here, and try again.
                # Use the application default credentials.
                cred = credentials.ApplicationDefault()

                firebase_admin.initialize_app(cred)
                logger.info("Initialized Firebase App.")

                if attempts > 4:
                    raise RuntimeError(
                        "Exceeded number of retries to get firestore client."
                    ) from e

            # time.sleep(get_wait_interval(attempts))
            time.sleep(5)

            attempts += 1

        return db
    
    def get_gcs_client(self) -> storage.Client:
        try:
            gcs = storage.Client(project=get_config().gcs_project)
        except ValueError as e:
            logger.error(f"Error initializing GCS client: {e}.")

        return gcs
    
    def _start_download(self, config_name: str, client_name: str):
        result: WriteResult = (
            self._get_db().collection(get_config().download_collection)
            .document(config_name)
            .set({"config_name": config_name, "client_name": client_name})
        )

        logger.info(
            f"Added {config_name} in 'download' collection. Update_time: {result.update_time}."
        )
    
    def _update_queues_on_start_download(self, config_name: str, licenses: list):
        for license in licenses:
            result: WriteResult = (
                self._get_db().collection(get_config().queues_collection)
                .document(license)
                .update({"queue": firestore.ArrayUnion([config_name])})
            )
            logger.info(
                f"Updated {license} queue in 'queues' collection. Update_time: {result.update_time}."
            )
    
    def _mark_partitioning_status(self, config_name: str, status: str):
        timestamp = (
            self._get_db().collection(get_config().download_collection)
            .document(config_name)
            .update({"status": status})
        )
        logger.info(
            f"Updated {config_name} in 'download' collection. Update_time: {timestamp}."
        )
    
    @contextlib.contextmanager
    def _open_local(self, file_name) -> t.Iterator[str]:
        bucket = self.get_gcs_client().get_bucket(get_config().storage_bucket)
        blob = bucket.blob(file_name)
        with tempfile.NamedTemporaryFile() as dest_file:
            blob.download_to_filename(dest_file.name)
            yield dest_file.name


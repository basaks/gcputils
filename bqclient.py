from typing import Union
import logging
from pathlib import Path
import pandas as pd
from google.auth import default
from google.cloud import bigquery
from google.cloud.bigquery import Client, TableReference, DatasetReference, \
    WriteDisposition, LoadJobConfig
from google.cloud.bigquery_storage_v1beta1 import BigQueryStorageClient
from google.cloud.exceptions import NotFound

log = logging.getLogger(__name__)


class BQClient:

    def __init__(self):
        """
        Explicitly create a credentials object. This allows you to use the same
        credentials for both the BigQuery and BigQuery Storage clients, avoiding
        unnecessary API calls to fetch duplicate authentication tokens.
        """
        credentials, your_project_id = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = Client(credentials=credentials, project=your_project_id)
        self.storage_client = BigQueryStorageClient(credentials=credentials)

    def table_exists(self, table_reference: TableReference) -> bool:
        """
        Args:
            table_reference (google.cloud.bigquery.table.TableReference):
                A reference to the table to look for.
        Returns:
            bool: ``True`` if the table exists, ``False`` otherwise.
        """
        try:
            self.client.get_table(table_reference)
            return True
        except NotFound:
            return False

    def dataset_exists(self, dataset_reference: DatasetReference) -> bool:
        """
        copied from:
        https://github.com/googleapis/google-cloud-python/blob/7ba0220ff9d0d68241baa863b3152c34dc9f7a1a/bigquery/docs/snippets.py#L178
        Return True if a dataset exists.

        Args:
            dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
                A reference to the dataset to look for.

        Returns:
            bool: ``True`` if the dataset exists, ``False`` otherwise.
        """
        try:
            self.client.get_dataset(dataset_reference)
            return True
        except NotFound:
            return False

    def sql_to_df(self, sql: str) -> pd.DataFrame:
        """
        download bq data via sql into df
        """
        download_job = self.client.query(sql)
        log.info(f"Starting to execute sql: {download_job.job_id}")
        df = download_job.to_dataframe()
        assert download_job.state == "DONE"  # safety
        log.info(f"Sql job complete: {download_job.job_id}")
        return df

    def df_to_bq(self, df: pd.DataFrame, table_reference: TableReference,
                 write_disposition: str = WriteDisposition.WRITE_EMPTY) \
            -> None:
        """
        Upload df to bq table
        Note 1: A similar functionality in BQ python api:
        load_table_from_dataframe. Currently there is no option to drop
        dataframe index being written in BQ table.
        Note 2: We write df into disc first using index=False. This avoids
        writing the dataframe index into the BQ table.
        """
        tmp_parquet = Path('tmp.parquet')
        df.to_parquet(tmp_parquet, index=False)

        self.parquet_to_bq(tmp_parquet, table_reference, write_disposition)
        tmp_parquet.unlink()

    def parquet_to_bq(self, parquet_df: Union[str, Path],
                      table_reference: TableReference,
                      write_disposition: str = WriteDisposition.WRITE_EMPTY) \
            -> None:
        """
        Upload parquet data frame on disc into BQ
        """

        # cast str to path
        if isinstance(parquet_df, str):
            parquet_df = Path(parquet_df)

        job_config = LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition
        )
        with open(parquet_df.as_posix(), 'rb') as f:
            load_job = self.client.load_table_from_file(
                f, table_reference, job_config=job_config)
        log.info(f"Starting  upload job job id: {load_job.job_id}")
        load_job.result()  # Waits for table load to complete.
        assert load_job.state == "DONE"  # safety
        log.info(f"Upload Job finished.")

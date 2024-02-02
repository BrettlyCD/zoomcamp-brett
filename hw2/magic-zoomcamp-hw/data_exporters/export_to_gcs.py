import pyarrow as pa
import pyarrow.parquet as pq

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_google_cloud_storage(data, *args, **kwargs):

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    project_id = "dezc-mage-brett"
    bucket_name = 'green_taxi_data_brett_hw2'
    table_name = 'green_taxi'

    root_path = f"{bucket_name}/{table_name}"

    print(root_path)

    # #define pyarrow table
    # table = pa.Table.from_pandas(df)

    # #find storage object - pulls in env variable automatically
    # gcs = pa.fs.GcsFileSystem()

    # #use parquet write to dataset
    # pq.write_to_dataset(
    #     table,
    #     root_path = root_path,
    #     partition_cols=['lpep_pickup_date'],
    #     filesystem=gcs
    # )


    # GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
    #     df,
    #     bucket_name,
    #     object_key,
    # )

import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/keys/dezc-mage-brett-ef97c58b01b6.json"

bucket_name = "mage-zoomcamp-brett"
project_id = "dezc-mage-brett"

table_name = "nyc_taxi_data"

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    #create date column from datetime
    data['tpep_pickup_date'] = data['tpep_pickup_datetime'].dt.date

    #define pyarrow table
    table = pa.Table.from_pandas(data)

    #find storage object - pulls in env variable automatically
    gcs = pa.fs.GcsFileSystem()

    #use parquet write to dataset
    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols=['tpep_pickup_date'], #has to be list
        filesystem=gcs
    )
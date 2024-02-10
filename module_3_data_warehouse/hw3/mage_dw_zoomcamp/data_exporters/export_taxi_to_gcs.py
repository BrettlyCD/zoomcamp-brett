import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/keys/dezc-mage-brett-ef97c58b01b6.json"

bucket_name = "taxi_data_2022_brettly"
project_id = "dezc-mage-brett"
table_name = "nyc_taxi_data_2022"

root_path = f"{bucket_name}/{table_name}"

@data_exporter
def export_data(data, *args, **kwargs):
    #create date column from datetime
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #define pyarrow table
    table = pa.Table.from_pandas(data)

    #find storage object - pulls in env variable automatically
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'], #has to be list
        filesystem=gcs,
        use_deprecated_int96_timestamps=True) #to get into a readable datetime instead of unix
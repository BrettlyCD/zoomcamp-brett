import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    url_base = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_'
    file_type = '.csv.gz'

    taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'lpep_pickup_datetime': str,
                    'lpep_dropoff_datetime': str,
                    'store_and_fwd_flag': str,
                    'RatecodeID':pd.Int64Dtype(),
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'passenger_count': pd.Int64Dtype(),
                    'trip_distance': float,
                    'fare_amount': float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'ehail_fee':float,
                    'improvement_surcharge':float,
                    'total_amount':float,
                    'payment_type': pd.Int64Dtype(),
                    'trip_type': pd.Int64Dtype(),
                    'congestion_surcharge':float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    #initialize list to store dfs
    dfs = []

    #iterate and load dataframes
    for month in ['2020-10', '2020-11', '2020-12']:
        df = pd.read_csv(url_base+month+file_type, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

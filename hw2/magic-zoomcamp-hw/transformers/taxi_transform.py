import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Remove records with 0 passengers OR trip distance
    data = data[(data['passenger_count']!=0) & (data['trip_distance']!=0)]

    print(f'{len(data)} records remaining.')

    #add date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    print(data['VendorID'].value_counts())

    #convert to snake case
    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.lower()
    )

    return data


@test
def test_vendor_id_exisits(output):
    assert 'vendor_id' in output.columns, 'vendor_id does not exist in the dataframe.'
    
@test
def test_passenger_count(output): 
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers.'
    
@test
def test_trip_distance(output):
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance.'
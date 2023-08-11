# taxi_data_loader
import io
import pandas as pd
import boto3

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_s3(*args, **kwargs):
    """
    Template for loading data from S3 bucket using AWS Boto3 library
    """
    bucket_name = 'ny-taxi-project-rht'
    key = 'yellow_tripdata_2023-01.parquet'

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = obj['Body'].read()

    return pd.read_parquet(io.BytesIO(body))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# taxi_data_transformer
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
    # Create the trip_fact table
    trip_fact = data.copy()
    trip_fact.rename(
        columns={
            "VendorID": "vendor_id",
            "RatecodeID": "rate_code_id",
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id"
        }
    )

    return trip_fact


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# taxi_zone_loader
import io
import pandas as pd
import boto3

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_s3(*args, **kwargs):
    """
    Template for loading data from S3 bucket using AWS Boto3 library
    """
    bucket_name = 'ny-taxi-project-rht'
    key = 'taxi_zones/taxi_zone_lookup.csv'

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = obj['Body'].read()

    return pd.read_csv(io.BytesIO(body), sep=',', low_memory=False)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# taxi_zone_transformer
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
    location_dim = data.copy()
    # Standardise the column names
    location_dim.columns = location_dim.columns.str.replace(' ', '_').str.lower()
    location_dim.rename(columns={"locationid": "location_id"}, inplace=True)
    

    return location_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# taxi_geometry_loader
import io
import pandas as pd
import geopandas as gpd
import boto3
import zipfile

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_s3(*args, **kwargs):
    """
    Template for loading data from S3 bucket using AWS Boto3 library
    """
    bucket_name = 'ny-taxi-project-rht'
    key = 'taxi_zones/taxi_zones.zip'

    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    body = obj['Body'].read()

    return gpd.read_file(io.BytesIO(body))

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# taxi_geometry_transformer
import pandas as pd
import geopandas as gpd
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
    # Specify your transformation logic here

    # Add centroid of each geometry as a new column
    print(data.dtypes)
    location_geom_dim = (
        data[["LocationID", "geometry"]]
        .rename(columns={"LocationID": "location_id"})
    )

    return location_geom_dim


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# centroid_transformer
import pandas as pd
import geopandas as gpd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, data2, *args, **kwargs):
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
    # Specify your transformation logic here
    data2["centroid_lat"] = data["geometry"].centroid.apply(lambda point: point.y)
    data2["centroid_long"] = data["geometry"].centroid.apply(lambda point: point.x)
    return data2


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

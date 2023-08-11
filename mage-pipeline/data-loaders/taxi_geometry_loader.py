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
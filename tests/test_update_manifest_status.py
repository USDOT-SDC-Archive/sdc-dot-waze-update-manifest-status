import os
import time

import boto3
import pytest
from moto import mock_dynamodb2

from lambdas.update_manifest_status_handler import ManifestHandler

manifest_table_name = "mock-dev-table"
manifest_index_name = "mock_index_name"
os.environ["DDB_MANIFEST_TABLE_ARN"] = "mock:aws:dynamodb:us-east-1::table/mock-dev-table"
os.environ["DDB_MANIFEST_FILES_INDEX_NAME"] = manifest_index_name


@mock_dynamodb2
def create_mock_table():
    ddb = boto3.client('dynamodb', region_name='us-east-1')
    ddb.create_table(AttributeDefinitions=[{
        'AttributeName': 'ManifestId',
        'AttributeType': 'S'
    },
        {
            'AttributeName': 'BatchId',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'TableName',
            'AttributeType': 'S'
        }],
        TableName=manifest_table_name,
        KeySchema=[
            {
                'AttributeName': 'ManifestId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'BatchId',
                'KeyType': 'RANGE'
            }
        ],
        GlobalSecondaryIndexes=[{
            'IndexName': manifest_index_name,
            'KeySchema': [
                {
                    'AttributeName': 'BatchId',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'TableName',
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL',
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 15,
                'WriteCapacityUnits': 20
            }
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 10
        },
    )


@mock_dynamodb2
def test_update_manifest_status_raises_exception():
    ddb_manifest_table_arn = os.environ.pop('DDB_MANIFEST_TABLE_ARN')
    try:
        create_mock_table()
        with pytest.raises(Exception):
            test_event = dict()
            test_event["batchId"] = str(int(time.time()))
            test_event["tablename"] = "alert"
            update_manifest_obj = ManifestHandler()
            update_manifest_obj.update_manifest_status(test_event)
    finally:
        os.environ['DDB_MANIFEST_TABLE_ARN'] = ddb_manifest_table_arn


@mock_dynamodb2
def test_update_manifest_status():
    create_mock_table()
    test_event = dict()
    test_event["batchId"] = str(int(time.time()))
    test_event["tablename"] = "alert"
    update_manifest_obj = ManifestHandler()
    update_manifest_obj.update_manifest_status(test_event)


@mock_dynamodb2
def test_update_manifest_status_with_item():
    create_mock_table()
    ddb_res = boto3.resource('dynamodb', region_name='us-east-1')
    batch_id = '17476592384'
    table = ddb_res.Table(manifest_table_name)
    table.put_item(Item={
        'ManifestId': 'dev-BatchId-TableName-index',
        'BatchId': batch_id,
        'TableName': manifest_table_name,
        'FileStatus': 'open'
    })
    test_event = dict()
    test_event["batchId"] = batch_id
    test_event["tablename"] = manifest_table_name
    update_manifest_obj = ManifestHandler()
    update_manifest_obj.update_manifest(test_event, None)

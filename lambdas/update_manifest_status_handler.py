import os

import boto3
from boto3.dynamodb.conditions import Attr, Key

from common.logger_utility import LoggerUtility


class ManifestHandler:

    def update_manifest_status(self, event, context):

        LoggerUtility.log_info("context: {}".format(context))
        batch_id = ""
        table_name = ""
        try:
            session = boto3.session.Session()
            ddb = session.resource('dynamodb', region_name='us-east-1')
            ddb_table_name = os.environ['DDB_MANIFEST_TABLE_ARN'].split('/')[1]
            manifest_index_name = os.environ['DDB_MANIFEST_FILES_INDEX_NAME']
            table_name = event['tablename']
            batch_id = event['batchId']
            ddb_table = ddb.Table(ddb_table_name)
            response = ddb_table.query(
                IndexName=manifest_index_name,
                KeyConditionExpression=Key('BatchId').eq(batch_id) & Key('TableName').eq(table_name),
                FilterExpression=Attr('FileStatus').eq('open')
            )

            if response['Count'] > 0:
                for item in response['Items']:
                    if table_name == item['TableName']:
                        ddb_table.update_item(
                            Key={'ManifestId': item['ManifestId'], 'BatchId': batch_id},
                            UpdateExpression='set FileStatus = :f',
                            ExpressionAttributeValues={':f': 'completed'}
                        )
                        LoggerUtility.log_error(
                            "Updated manifest status for  batchId {} and table {}".format(batch_id, table_name))
                        break
        except Exception as e:
            LoggerUtility.log_error(
                "Unable to update manifest status for  batchId {} and table {}".format(batch_id, table_name))
            raise e

    def update_manifest(self, event, context):
        self.update_manifest_status(event, context)

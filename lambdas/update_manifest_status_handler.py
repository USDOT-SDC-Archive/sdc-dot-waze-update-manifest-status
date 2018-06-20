import boto3
import json
import os
from common.logger_utility import *
from common.constants import *
from boto3.dynamodb.conditions import Attr, Key

class ManifestHandler:

    def __update_manifest_status(self,event, context):
        try:
            session = boto3.session.Session()
            ddb = session.resource('dynamodb')
            ddb_table_name = os.environ['DDB_MANIFEST_TABLE_ARN'].split('/')[1]
            manifest_index_name = os.environ['DDB_MANIFEST_FILES_INDEX_NAME']
            table_name = event['tablename']
            batch_id=event['batchId']
            ddb_table = ddb.Table(ddb_table_name)
            response = ddb_table.query(IndexName=manifest_index_name,
                                       KeyConditionExpression=Key('BatchId'
                                       ).eq(batch_id) & Key('TableName'
                                       ).eq(table_name),
                                       FilterExpression=Attr('FileStatus'
                                       ).eq('open'))
        
            if response['Count'] > 0:
                for item in response['Items']:
                    tableName = item['TableName']
                    if tableName == table_name:
                        response = ddb_table.update_item(Key={'ManifestId': item['ManifestId'
                                ], 'BatchId': batch_id},
                                UpdateExpression='set FileStatus = :f',
                                ExpressionAttributeValues={':f': 'completed'})
                        LoggerUtility.logError("Updated manifest status for  batchId {} and table {}".format(batch_id,table_name))
                        break
        except Exception as e:
            LoggerUtility.logError("Unable to update manifest status for  batchId {} and table {}".format(batch_id,table_name))
            raise e
    
    def update_manifest_(self, event, context):
        self.__update_manifest_status(event, context)
        

from lambdas.update_manifest_status_handler import *
from common.logger_utility import *
from common.constants import *

def lambda_handler(event, context):
    LoggerUtility.setLevel()
    update_manifest_handle_event = ManifestHandler()
    update_manifest_handle_event.update_manifest(event, context)
    return event
from common.logger_utility import LoggerUtility
from lambdas.update_manifest_status_handler import ManifestHandler


def lambda_handler(event, context):
    LoggerUtility.set_level()
    update_manifest_handle_event = ManifestHandler()
    update_manifest_handle_event.update_manifest(event, context)
    return event

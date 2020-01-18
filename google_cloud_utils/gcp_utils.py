from google.cloud import storage
from config import log_splice_config as ls_config
from utils.las_writer import spliced_las_writer
from google.oauth2 import service_account
import os

# GCP
def get_blob_list(las_bucket, source_bucket):
    blob_list = source_bucket.list_blobs()
    list_blob = [las_bucket + "/" + single_blob.name for single_blob in blob_list]
    return list_blob


def get_gcp_bucket(bucket, credentials=None):
    credentials = service_account.Credentials.from_service_account_file(ls_config.CREDENTIALS_JSON)

    gcp_client1 = storage.Client(credentials=credentials)
    gcp_bucket = gcp_client1.get_bucket(bucket)
    return gcp_bucket


def gcp_download_blob2(las_path, tmp_las_path, credentials):
    las_bucket = las_path.split("/")[0]
    gcp_client_tmp = storage.Client(credentials=credentials)
    source_bucket = gcp_client_tmp.get_bucket(las_bucket)
    blob_object = source_bucket.blob(
        las_path.replace(las_bucket + "/", ""))
    blob_object.download_to_filename(tmp_las_path)
    return None


def gcp_download_blob(blob, source_bucket, tmp_las_path):
    blob_object = source_bucket.blob(blob)
    blob_object.download_to_filename(tmp_las_path)
    return None


def gcp_upload_blob(ipt_bucket, blob, source_bucket, jobid, file=None):
    blob_object = source_bucket.blob(blob)
    metadata = {"job_id": jobid, "las_bucket": ipt_bucket}
    blob_object.metadata = metadata
    if file:
        blob_object.upload_from_filename(file)
    else:
        blob_object.upload_from_string("")


def gcp_output_merged_las(las_bucket, uwi, destination_bucket, job_id, w):
    log_file_name_with_path = las_bucket + \
                              "/" + "UWI_" + str(uwi) + ".las"
    tmp_log_file_name_with_path = (
            ls_config.DATA_PATH + "/" + "UWI_" + str(uwi) + ".las")
    spliced_las_writer(tmp_log_file_name_with_path, w)
    gcp_upload_blob(las_bucket, log_file_name_with_path, destination_bucket, job_id, tmp_log_file_name_with_path)
    os.remove(tmp_log_file_name_with_path)



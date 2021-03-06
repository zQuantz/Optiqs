from const import DIR, DATE, CONFIG, CREDS, logger
from utils import send_email, send_metric
from datetime import datetime, timedelta
from google.cloud import storage
from zipfile import ZipFile
from hashlib import md5
import tarfile as tar
import boto3 as boto
import sys, os
import base64
import pysftp
import pytz
import time

###################################################################################################

CBOE = CONFIG['CBOE']
GCP = CONFIG['GCP']

storage_client = storage.Client(credentials=CREDS)
GCP_BUCKET = storage_client.bucket(GCP['BUCKET'])
GCP_VAULT = storage_client.bucket(GCP['VAULT'])
AWS_VAULT = boto.resource('s3').Bucket("cboe-options-vault")

CLOUD_FNAME = f"{SDATE}.tar.xz"
TAR_FNAME = f"{DIR}/data/{CLOUD_FNAME}"

###################################################################################################

def download_and_compress():

	localname = f"{DIR}/data/{SDATE}.zip"
	host = CBOE['HOST']
	username = CBOE['USER']
	password = CBOE['PASS']

	logger.info(f"Connecting to SFTP Server: {host}")
	
	cnopts = pysftp.CnOpts()
	cnopts.hostkeys = None

	deadline = datetime.now() + timedelta(hours=5)
	while datetime.now() < deadline:

		try:

			with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:

				sftp.chdir(CBOE['PATH'])

				logger.info(f"Downloading: {CBOE['FNAME']}{SDATE}.zip")
				sftp.get(f"{CBOE['FNAME']}{SDATE}.zip", localpath=localname)

				filesize = os.stat(localname).st_size / 1_000_000
				logger.info(f"Size of file: {filesize}mbs")
				send_metric(CONFIG, "cboe_options_dump_size", "double_value", filesize)
				send_email(CONFIG, "CBOE File Download", f"""Success! File Size: {round(filesize, 2)}mbs""", [], logger)
				break

		except FileNotFoundError as not_found:

			logger.info("Not uploaded yet. Sleeping...")
			time.sleep(300)

		except Exception as e:

			logger.warning(f"Error. {e}. Exiting.")
			1/0

	assert datetime.now() > deadline, "File not uploaded. Deadline met."

	logger.info(f"Unzipping data...")
	with ZipFile(localname, "r") as zip_file:
		zip_file.extractall(path=f"{DIR}/data")

	logger.info(f"Renameing file to: {DIR}/data/{SDATE}.csv")
	os.rename(f"{DIR}/data/{CBOE['FNAME']}{SDATE}.csv", f"{DIR}/data/{SDATE}.csv")

	logger.info("Compressing file with tar...")
	with tar.open(TAR_FNAME, "x:xz") as tar_file:
		tar_file.add(f"{DIR}/data/{SDATE}.csv", arcname=f"{SDATE}.csv")

	logger.info("Deleting zip & csv...")
	os.unlink(f"{DIR}/data/{SDATE}.zip")
	os.unlink(f"{DIR}/data/{SDATE}.csv")

def save_to_cloud():

	blob = GCP_BUCKET.blob(CLOUD_FNAME)
	if not blob.exists():
		logger.info("Uploading blob...")
		blob.upload_from_filename(TAR_FNAME, checksum="md5")
		logger.info("Blob uploaded.")
	else:
		logger.warning(f"Error. {CLOUD_FNAME} blob exists.")

	vault_blob = GCP_VAULT.blob(CLOUD_FNAME)
	if not vault_blob.exists():
		logger.info("Uploading vault blob...")
		vault_blob.upload_from_filename(TAR_FNAME, checksum="md5")
		logger.info("Vault blob uploaded.")
	else:
		logger.warning(f"Error. {CLOUD_FNAME} vault blob exists.")

	with open(TAR_FNAME, "rb") as file:
		body = file.read()

	_hash = md5(body).digest()
	_hash = base64.b64encode(_hash).decode()
	logger.info(f"AWS Base64-encoded MD5 hash: {_hash}")

	logger.info("Uploading vault object...")
	AWS_VAULT.put_object(Key=CLOUD_FNAME, Body=body, ContentMD5=_hash, StorageClass='GLACIER')
	logger.info("Vault object uploaded.")

if __name__ == '__main__':

	logger.info("CBOE Downloader Initialized.")

	try:
		download_and_compress()
		save_to_cloud()
		send_metric(CONFIG, "cboe_options_indicator", "int64_value", 1)
	except Exception as e:
		logger.warning(f"Process Error. {e}")
		send_metric(CONFIG, "cboe_options_indicator", "int64_value", 0)

	logger.info("CBOE Downloader Terminated.")

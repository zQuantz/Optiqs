from const import DIR, DATE, CONFIG, CREDS, logger
from google.cloud import storage
from gcp import send_metric
from zipfile import ZipFile
import tarfile as tar
import boto3 as boto
import sys, os
import pysftp
import json

###################################################################################################

CBOE = CONFIG['CBOE']
GCP = CONFIG['GCP']

storage_client = storage.Client(credentials=CREDS)
GCP_BUCKET = storage_client.bucket(GCP['BUCKET'])
GCP_VAULT = storage_client.bucket(GCP['VAULT'])
AWS_VAULT = boto.resource('s3').bucket("cboe-options-vault")

###################################################################################################

def download_and_compress():

	localname = f"{DIR}/data/{DATE}.zip"
	host = CBOE['HOST']
	username = CBOE['USER']
	password = CBOE['PASS']

	logger.info(f"Connecting to SFTP Server: {host}")
	with pysftp.Connection(host, username=username, password=password) as sftp:

		sftp.chdir(CBOE['PATH'])

		logger.info(f"Downloading: {CBOE['FNAME']}{DATE}.zip")
		sftp.get(f"{CBOE['FNAME']}{DATE}.zip", localpath=localname)

		filesize = os.stat(localname).st_size / 1_000_000
		logger.info(f"Size of file: {filesize}mbs")

	logger.info(f"Unzipping data...")
	with ZipFile(localname, "r") as zip_file:
		zip_file.extractall(path=f"{DIR}/data")

	logger.info(f"Renameing file to: {DIR}/data/{DATE}.csv")
	os.rename(f"{DIR}/data/{CBOE['FNAME']}{DATE}.csv", f"{DIR}/data/{DATE}.csv")

	logger.info("Compressing file with tar...")
	with tar.open(f"{DIR}/data/{DATE}.tar.xz", "x:xz") as tar_file:
		tar_file.add(f"{DIR}/data/{DATE}.csv", arcname=f"{DATE}.csv")

	logger.info("Deleting zip...")
	os.unlink(f"{DIR}/data/{DATE}.zip")

def store():

	blob = GCP_BUCKET.blob(f"{DATE}.tar.xz")
	if not blob.exists():
		logger.warning(f"Error. {DATE}.tar.xz blob exists.")

	vault_blob = GCP_VAULT.blob(f"")
	if not vault_blob.exists():
		logger.warning(f"Error. {DATE}.tar.xz vault blob exists.")

	logger.info("Uploading blob...")
	blob.upload_from_filename(f"{DIR}/data/{DATE}.tar.xz", checksum="md5")
	logger.info("Blob uploaded.")

	logger.info("Uploading vault blob...")
	vault_blob.upload_from_filename(f"{DIR}/data/{DATE}.tar.xz", checksum="md5")
	logger.info("Vault blob uploaded.")

if __name__ == '__main__':

	logger.info("CBOE Downloader Initialized")

	try:
		download_and_compress()
		send_metric("cboe_options_indicator", "int64_value", 1)
	except Exception as e:
		logger.warning(f"Process Error. {e}")
		send_metric("cboe_options_indicator", "int64_value", 0)
from zipfile import ZipFile
import tarfile as tar

import sys, os
import pysftp
import json

DIR = os.path.realpath(os.path.dirname(__file__))
DATE = "2021-01-19"

with open(f"{DIR}/optiqs_config.json", "r") as file:
	CONFIG = json.loads(file.read())

def download_and_compress():

	localname = f"{DIR}/data/{DATE}.zip"
	host = CONFIG['CBOE_HOST']
	username = CONFIG['CBOE_USER']
	password = CONFIG['CBOE_PASS']

	with pysftp.Connection(host, username=username, password=password) as sftp:

		sftp.chdir(CONFIG['CBOE_PATH'])
		sftp.get(f"{CONFIG['CBOE_FNAME']}{DATE}.zip", localpath=localname)

		filesize = os.stat(localname).st_size / 1_000_000
		print(f"Size of data: {filesize}mbs")

	with ZipFile(localname, "r") as zip_file:
		zip_file.extractall(path=f"{DIR}/data")

	os.rename(f"{DIR}/data/{CONFIG['CBOE_FNAME']}{DATE}.csv", f"{DIR}/data/{DATE}.csv")

	with tar.open(f"{DIR}/data/{DATE}.tar.xz", "x:xz") as tar_file:
		tar_file.add(f"{DIR}/data/{DATE}.csv", arcname=f"{DATE}.csv")

	os.unlink(f"{DIR}/data/{DATE}.zip")

if __name__ == '__main__':

	download_and_compress()

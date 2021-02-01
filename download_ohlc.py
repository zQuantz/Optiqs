from const import CONFIG, DIR, CREDS, DATE, SDATE, logger
from datetime import datetime, timedelta
from utils import send_metric, request
from google.cloud import storage
from pathlib import Path
import tarfile as tar
import pandas as pd
import requests
import sys, os
import json

###################################################################################################

CLIENT = storage.Client(credentials=CREDS)
BUCKET = CLIENT.bucket(CONFIG['GCP']['BUCKET'])
OHLC_BUCKET = CLIENT.bucket(CONFIG['GCP']['OHLC_BUCKET'])

URL = "https://query1.finance.yahoo.com/v7/finance/download/{TICKER}?"
FIRST_ROW = "date,open,high,low,close,adjclose,volume,ticker\n"

PARAMS = {
	"historical" : {
		"D1" : int(datetime(2018, 1, 29, 18).timestamp()),
		"D2" : int(datetime(2021, 1, 29, 18).timestamp()),
		"IDX" : 0,
		"FNAME" : "2021-01-29"
	},
	"live" : {
		"D1" : int((DATE - timedelta(days=7)).timestamp()),
		"D2" : int(DATE.timestamp()),
		"IDX" : -1,
		"FNAME" : SDATE
	}
}

METHOD = "live"
D1 = PARAMS[METHOD]['D1']
D2 = PARAMS[METHOD]['D2']
URL += f"period1={D1}&period2={D2}&interval=1d&events=history&includeAdjustedClose=true"

###################################################################################################

def get_tickers():

	blob = BUCKET.blob(PARAMS[METHOD]['FNAME']+".tar.xz")
	if not blob.exists():
		print("File does not exist. Using backup.")
		blob = list(BUCKET.list_blobs())[PARAMS[METHOD]['IDX']]

	file = Path(f"{DIR}/data/{blob.name}")
	logger.info(f"Using: {file.name}")
	blob.download_to_filename(file)

	with tar.open(file, "r:xz") as tar_file:
		tar_file.extractall(file.parent)

	df = pd.read_csv(file.with_suffix("").with_suffix(".csv"))
	tickers = df.underlying_symbol.unique()

	os.unlink(file)
	os.unlink(file.with_suffix("").with_suffix(".csv"))

	return tickers

def download(tickers):

	data = FIRST_ROW

	for ticker in tickers:

		if "^" in ticker:
			continue

		url = URL.format(TICKER=ticker.replace(".", "-"))
		resp = request(url)

		if resp.status_code != 200:
			logger.warning(f"Non-200 Status Code. Ticker: {ticker}. Code: {resp.status_code}. Reason: {resp.reason}")
			continue

		idx = resp.text.find("\n")
		data += resp.text[idx+1:].replace("\n", f",{ticker}\n")
		data += f",{ticker}\n"

	return data[:-1]

def save(data):

	csv_file = Path(f"{DIR}/data/{PARAMS[METHOD]['FNAME']}.csv")
	xz_file = csv_file.with_suffix(".tar.xz")
	
	with open(csv_file, "w") as file:
		file.write(data)

	if METHOD == "live":
		data = pd.read_csv(csv_file)
		data = data[data.date == SDATE]
		data.to_csv(csv_file, index=False)

	with tar.open(xz_file, "x:xz") as tar_file:
		tar_file.add(csv_file, arcname=csv_file.name)

	blob = OHLC_BUCKET.blob(xz_file.name)
	logger.info("Uploading blob...")
	blob.upload_from_filename(xz_file, checksum="md5")
	logger.info("Blob uploaded.")

	os.unlink(csv_file)
	os.unlink(xz_file)

if __name__ == '__main__':

	logger.info("OHLC Downloader Initialized.")

	try:

		tickers = get_tickers()
		data = download(tickers)

		metric = len(data.split("\n")) / len(tickers)
		if METHOD == "live":
			send_metric(CONFIG, "ohlc_download_counter", "double_value", metric)
		else:
			print(metric)

		save(data)
		send_metric(CONFIG, "ohlc_download_indicator", "int64_value", 1)

	except Exception as e:

		send_metric(CONFIG, "ohlc_download_indicator", "int64_value", 0)
		logger.warning(f"Fatal Error. {e}")

	logger.info("OHLC Downloader Terminated.")

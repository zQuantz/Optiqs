from google.oauth2.service_account import Credentials
import logging
import json
import os

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
DATE = "2021-01-20"

with open(f"{DIR}/optiqs_config.json", "r") as file:
	CONFIG = json.loads(file.read())

CREDS = Credentials.from_service_account_file(os.environ.get(CONFIG['GCP']['ENV_CREDS_KEY']))

###################################################################################################

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/log.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################

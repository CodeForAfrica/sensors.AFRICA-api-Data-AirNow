import os

from dotenv import load_dotenv

load_dotenv()

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "cfa-airquality")
S3_OBJECT_KEY = os.environ["S3_OBJECT_KEY"]
S3_HISTORY_OBJECT_KEY = os.environ["S3_HISTORY_OBJECT_KEY"]
HISTORY_SCHEDULE_RATE = os.getenv("HISTORY_SCHEDULE_RATE", 1)
SCHEDULE_RATE = os.getenv("SCHEDULE_RATE", 1)
SENSORS_AFRICA_API = os.environ.get("SENSORS_AFRICA_API", "http://127.0.0.1:8000")
SENSORS_AFRICA_AUTH_TOKEN = os.environ['SENSORS_AFRICA_AUTH_TOKEN']
SENTRY_DSN = os.getenv('SENTRY_DSN')
AIRNOW_API_KEY = os.environ["AIRNOW_API_KEY"]
LAST_DATA_RECORDED_ON = os.environ["LAST_DATA_RECORDED_ON"]
OWNER_ID = os.environ['OWNER_ID']

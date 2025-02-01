import os
import sys
from botocore.config import Config
from datetime import datetime, timedelta
from functions import datefromfilename

before = datetime.now()

test, pretend = False, False

if len(sys.argv) > 1:
    if sys.argv[1] == 'test': test = True
    elif sys.argv[1] == 'pretend': pretend = True

key_id = os.getenv('AWS_ACCESS_KEY_ID')
acces_key = os.getenv('AWS_SECRET_ACCESS_KEY')

session = boto3.Session(
    aws_access_key_id=key_id,
    aws_secret_access_key=acces_key,
)

s3 = session.client(
    "s3",
    endpoint_url="https://files.polygon.io",
    config=Config(signature_version="s3v4"),
)

paginator = s3.get_paginator("list_objects_v2")
prefix = "us_stocks_sip/day_aggs_v1/"
bucket = "flatfiles"
allfilenames = []

print("Connected to Client\n")
print("Paginating...")

for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page["Contents"]:
        allfilenames.append(obj["Key"])

category = [i.split("/")[1] for i in allfilenames]
# stonkfilenames = [i for i in allfilenames if i.split("/")[1] == "day_aggs_v1"]
datestrings = [i.split("/")[-1].split(".")[0] for i in allfilenames]
date = [datetime.date(datetime.strptime(i, "%Y-%m-%d")) for i in datestrings]

def get_last_line(file_name):
    with open(file_name, 'rb') as f:
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b'\n':
            f.seek(-2, os.SEEK_CUR)
        last_line = f.readline().decode()
    return last_line

if pretend:
    most_recent_date = (datetime.now() - timedelta(days=7)).date()
else:
    most_recent_date_string = get_last_line("all_dailies.csv").split(",")[-1].strip('\r\n"')
    most_recent_date = datetime.date(datetime.strptime(most_recent_date_string, "%Y-%m-%d"))

missingfiles = [i for i,j in zip(allfilenames, date) if j > most_recent_date]

if len(missingfiles) == 0: 
    after = datetime.now()
    print(f'Time taken: {after - before}')
    sys.exit("No missing files.")

print("Downloading...")

if not test:
    for i in missingfiles:
        localfilename = "day_aggs/" + i.split("/")[-1]

        try:
            s3.download_file(bucket, i, localfilename)
        except Exception as e:
            print(e)

for i in missingfiles:
    print(i)

print('______________________\n')
first, last = datefromfilename(missingfiles[0]), datefromfilename(missingfiles[-1])
print(f'Downloaded {len(missingfiles)} files between {first} and {last}')

after = datetime.now()
print(f'\nTime taken: {after - before}')
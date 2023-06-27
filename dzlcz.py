import twint.run
import os
import requests
from dotenv import load_dotenv
from twint import Config
import pandas as pd
from dataclasses import dataclass, asdict
from datetime import datetime
import json


@dataclass
class TweetReport:
    tweet_id: str
    tweet_user_id: str
    tweet_username: str
    reported_by_user_id: str
    reported_by_username: str
    reported_by_status_id: str
    reported_at: float

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

def create_tweet_report(row):
    datetime_format = "%Y-%m-%d %H:%M:%S"
    report = TweetReport(
        tweet_id=row['in_reply_to_status_id'],
        tweet_user_id=row['in_reply_to_user_id'],
        tweet_username=row['in_reply_to_username'],
        reported_by_user_id=row['user_id'],
        reported_by_username=row['username'],
        reported_by_status_id=row['id'],
        #send as timestamp (units: ms)
        reported_at=row['created_at']
    )
    return report

def strip_whitespace(x):
    return x.strip()

forbidden_usernames = []
with open("dzlcz_forbidden_ids.txt", "r") as file:
    forbidden_usernames = map(strip_whitespace, file.readlines())

load_dotenv()

x_csrf_token = os.environ.get("X_CSRF_TOKEN")
cookie = os.environ.get("COOKIE")

config = Config()

config.Search = "#dzlcz"
config.Impressions = True
config.Store_csv = True
config.Replies = True
config.Pandas = True
config.Limit = 100
config.X_csrf_token = x_csrf_token
config.Cookie = cookie

# twint.run.Search(config)
# 
# search_data = twint.storage.panda.Tweets_df
# 
# reports = search_data\
#     .query("username not in @forbidden_usernames")\
#     .query("in_reply_to_username not in @forbidden_usernames")\
#     .apply(create_tweet_report, axis=1)
# 
# # json_data = str(TweetReport.schema().dump(reports, many=True))
# reports_as_dict = [asdict(obj) for obj in reports]
# json_data = json.dumps(reports_as_dict)

json_data = "[{\"tweet_id\": \"1669368116046905346\", \"tweet_user_id\": \"1645463778085969923\", \"tweet_username\": \"RebelLibis\", \"reported_by_user_id\": 1366896190797840391, \"reported_by_username\": \"chuavexilu\", \"reported_by_status_id\": \"1670413185931771904\", \"reported_at\": 1687092512000.0}, {\"tweet_id\": \"1670399373606256641\", \"tweet_user_id\": \"1603344906440908803\", \"tweet_username\": \"Jiri_Cejle\", \"reported_by_user_id\": 1355186513210912773, \"reported_by_username\": \"JirkaSafra\", \"reported_by_status_id\": \"1670404775907663873\", \"reported_at\": 1687090507000.0}, {\"tweet_id\": \"1670363541503717378\", \"tweet_user_id\": \"1588521164103614465\", \"tweet_username\": \"radekmokry64\", \"reported_by_user_id\": 3329574471, \"reported_by_username\": \"kopapaka\", \"reported_by_status_id\": \"1670378219898388483\", \"reported_at\": 1687084176000.0}]"
json_data = '{"reports": ' + json_data + '}'
print(json_data)

with open("output/dzlcz.json", "w") as filewrite:
    filewrite.write(json_data)

host = os.environ.get("DZLCZ_HOST")
auth_token = os.environ.get("DZLCZ_AUTH_TOKEN")

headers = {
    'Authorization': f'Token {auth_token}'
}

response = requests.post(url=host, json=json_data, headers=headers)

print(response.status_code)
print(response.text)
#todo: send data to api
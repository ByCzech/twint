import twint.run
import os
from dotenv import load_dotenv
from twint import Config
import pandas as pd
from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class TweetReport:
    tweet_id: str
    tweet_user_id: str
    tweet_username: str
    reported_by_user_id: str
    reported_by_username: str
    reported_by_status_id: str

def create_tweet_report(row):
    report = TweetReport(
        tweet_id=row['in_reply_to_status_id'],
        tweet_user_id=row['in_reply_to_user_id'],
        tweet_username=row['in_reply_to_username'],
        reported_by_user_id=row['user_id'],
        reported_by_username=row['username'],
        reported_by_status_id=row['id']
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

twint.run.Search(config)

search_data = twint.storage.panda.Tweets_df

reports = search_data\
    .query("username not in @forbidden_usernames")\
    .query("in_reply_to_username not in @forbidden_usernames")\
    .apply(create_tweet_report, axis=1)

json_data = str(TweetReport.schema().dump(reports, many=True))
print(json_data)

with open("output/dzlcz.json", "w") as filewrite:
    filewrite.write(json_data)


#todo: send data to api
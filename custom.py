import twint.run
from twint import Config
import os
from dotenv import load_dotenv



config = Config()
config.User_id = "msam_cz"
# config.Search = "obsah"
config.Limit = 10
# config.deleted = True
# config.Replies = True
config.Replies = False
config.Impressions = True

# NOTE: will be indexed
config.Store_csv = True

## NOTE: reads tweets from the newest to the oldest
config.Since = "2023-04-01 20:30:15"
config.Until = "2023-05-01 20:00:00"
config.Output = "./output/impressions.csv"

load_dotenv()

x_csrf_token = os.environ.get("X_CSRF_TOKEN")
cookie = os.environ.get("COOKIE")

if x_csrf_token is None or cookie is None:
    print("Please specify correct X_CSRF_TOKEN and COOKIE values in .env file.")
else:
    config.X_csrf_token = x_csrf_token
    config.Cookie = cookie

    twint.run.Search(config)
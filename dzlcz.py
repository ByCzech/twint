import twint.run
import os
from dotenv import load_dotenv
from twint import Config

load_dotenv()

x_csrf_token = os.environ.get("X_CSRF_TOKEN")
cookie = os.environ.get("COOKIE")

config = Config()

config.Search = "#dzlcz"
config.Impressions = True
config.Store_csv = True
config.Replies = True
config.Output = "output/dzlcz.csv"
config.Limit = 100
config.X_csrf_token = x_csrf_token
config.Cookie = cookie

twint.run.Search(config)

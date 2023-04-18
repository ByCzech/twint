import twint.run
from twint import Config



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
config.Since = "2012-12-20 20:30:15"
config.Until = "2023-03-15 20:00:00"
config.Output = "./output/impressions.csv"

twint.run.Search(config)

import twint.run
from twint import Config
import pandas as pd

config = Config()
config.User_id = "msam_cz"
config.Followers = True
# config.Pandas = True
config.Pandas = True
twint.run.Lookup(config)

df = twint.storage.panda.User_df
print(df.loc[0, ['followers']].item())
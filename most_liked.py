import sys, os, time
from datetime import datetime, date, timedelta

import twint.run
from twint import Config
from dotenv import load_dotenv
import pandas as pd
import csv


def get_usernames(source_filename: str):
    return open(source_filename).read().splitlines()


def get_twint_config(username, start_datetime, end_datetime):
    config = Config()
    config.User_id = username
    config.Since = start_datetime
    config.Until = end_datetime
    config.Utc = False
    config.Replies = False
    config.Impressions = True
    config.Pandas = True
    config.X_csrf_token = os.environ.get("")
    
    return config

def get_tweets(source_names, start_time, end_time, x_csrf_token, cookie):
    # "id","conversation_id","created_at","date","timezone","place","tweet","language","hashtags","cashtags","user_id","user_id_str","username","name","day","hour","link","urls","photos","video","thumbnail","retweet","nlikes","nreplies","nretweets","nquotes","nimpressions","quote_url","search","near","geo","source","user_rt_id","user_rt","retweet_id","reply_to","retweet_date","translate","trans_src","trans_dest"
    # "1655163119059054595","1655163119059054595",1683456613000.0,"2023-05-07 12:50:13","+0200","","Dnes jsem měl tu čest odstartovat jeden z nejprestižnějších běžeckých závodů – Pražský maraton. Když vidím tu rozmanitou skladbu běžců, je to obrovská motivace nejen pro mě, ale i pro ostatní. Je zároveň přenášený živě, a tak je Praha na obrazovkách lidí po celém světě.  https://t.co/I092d97043","cs","[]","[]",4136554833,"4136554833","prezidentpavel","Petr Pavel",7,"12","https://twitter.com/prezidentpavel/status/1655163119059054595","[]","['https://pbs.twimg.com/media/FvhSFVnWYAEnW00.jpg']",1,"https://pbs.twimg.com/media/FvhSFVnWYAEnW00.jpg",False,3487,95,80,10,140944,"","None","","","","","","","[]","","","",""
    df = pd.DataFrame(columns=['user_id', 'username', 'name', 'tweet', 'date', 'nlikes', 'nimpressions', 'nquotes', 'nreplies', 'nretweets', 'reply_to'])
    for username in source_names:
        print(f"[{username}]] Getting tweets...")
        config = get_twint_config(username, start_time, end_time)
        config.X_csrf_token = x_csrf_token
        config.Cookie = cookie
        try:
            twint.run.Search(config)
            search_data = twint.storage.panda.Tweets_df
            # print(search_data)
            if df is None:
                df = search_data
            else:
                df = pd.concat([df, search_data], ignore_index=True)
            # output = f'{df["id"].iloc[0]},"{df["name"].iloc[0]}",{source_name},{df["tweets"].iloc[0]},{df["join_date"].iloc[0]},{df["avatar"].iloc[0]},{df["followers"].iloc[0]},{df["following"].iloc[0]}'
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(3)
    return df


def write_out_tweets(tweets_df, filename):
    # tweets_df.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)
    tweets_df \
        .sort_values(by=['nlikes'], ascending=False) \
        .to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC)



def get_last_week(date: date):
    last_week_date = date - timedelta(days=7)
    return get_week(last_week_date)


def get_week(date: date):
    year, week_num, day_of_week = date.isocalendar()
    return f'{year}-W{week_num}'


def get_datetime_iso(date: date, method):
    d1 = method(date)
    r1 = datetime.strptime(d1 + '-1', "%Y-W%W-%w")
    return 'T'.join(str(r1.astimezone()).split())

def get_tweets_once(api_client, source_ids):
    if (len(sys.argv) <= 3):
        week_date = date.today()
    else:
        # get_datetime_iso() always get the datetime of Monday 00:00 
        # so add 7 days to set the start of 'last week' as this week's monday
        week_date = datetime.strptime(sys.argv[3],"%Y-%m-%d") + timedelta(days=7)
    tweets = get_tweets(api_client, source_ids)
    write_out_tweets(tweets, sys.argv[2])


def get_tweets_for_week(source_names, output_filename, end_date, week_num, x_csrf_token, cookie):
    if not os.path.exists("./output/most_liked"):
        os.mkdir("./output/most_liked")
    
    year, end_week_num, day_of_week = end_date.isocalendar()
    start = datetime.strptime(f"{year}-W{week_num}" + '-1', "%Y-W%W-%w")
    start_str = str(start)
    end = datetime.strptime(f"{year}-W{week_num+1}" + '-1', "%Y-W%W-%w")
    end_str = str(end)
    print(f"------ GETTING RESULTS FOR WEEK {week_num} -------")
    print(f"------ FROM {start_str} TO {end_str}")
    tweets_df = get_tweets(source_names, start_str, end_str, x_csrf_token, cookie)
    write_out_tweets(tweets_df, output_filename)
    

def main():
    print("\n\n")
    
    if (len(sys.argv) < 3):
        print("NOT ENOUGH ARGUMENTS. PLEASE RUN THIS AS FOLLOWS:")
        print("python most_liked.py SOURCE_FILE WEEK_NUM OUTPUTFILE")
        print("\n")
        print("SOURCEFILE - text file with one username per line")
        print("WEEK_NUM - number of week in current year")
        return
    
    load_dotenv()

    x_csrf_token = os.environ.get("X_CSRF_TOKEN")
    cookie = os.environ.get("COOKIE")

    if x_csrf_token is None or cookie is None:
        print("Please specify correct X_CSRF_TOKEN and COOKIE values in .env file.")
        return
    
    source_filename = sys.argv[1]
    week_num = int(sys.argv[2])
    output_filename = sys.argv[3]
    source_names = get_usernames(source_filename)
    get_tweets_for_week(source_names, output_filename, date.today(), week_num, x_csrf_token, cookie)


if __name__ == "__main__":
    main()
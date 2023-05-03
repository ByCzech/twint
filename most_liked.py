import sys, os, time
from datetime import datetime, date, timedelta

import twint.run
from twint import Config
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
    return config

def get_tweets(source_names, start_time, end_time):
    df = pd.DataFrame(columns=['user_id', 'username', 'tweet_id', 'tweet_content', 'created_at', 'like_count', 'impression_count', 'quote_count', 'reply_count', 'retweet_count'])
    for username in source_names:
        print(f"[{username}]] Getting tweets...")
        config = get_twint_config(username, start_time, end_time)
        try:
            twint.run.Search(config)
            search_data = twint.storage.panda.User_df
            print(search_data)
            df.append(search_data)
            # output = f'{df["id"].iloc[0]},"{df["name"].iloc[0]}",{source_name},{df["tweets"].iloc[0]},{df["join_date"].iloc[0]},{df["avatar"].iloc[0]},{df["followers"].iloc[0]},{df["following"].iloc[0]}'
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(3)
    return df


def write_out_tweets(tweets_df, filename):
    tweets_df \
        .query("like_count > 9") \
        .sort_values(by=['like_count'], ascending=False) \
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


def get_tweets_for_week(source_names, end_date, week_num=1):
    if not os.path.exists("./output/most_liked"):
        os.mkdir("./output/most_liked")
    
    year, end_week_num, day_of_week = end_date.isocalendar()
    start = datetime.strptime(f"{year}-W{week_num}" + '-1', "%Y-W%W-%w")
    start_str = str(start)
    end = datetime.strptime(f"{year}-W{week_num+1}" + '-1', "%Y-W%W-%w")
    end_str = str(end)
    print(f"------ GETTING RESULTS FOR WEEK {week_num} -------")
    print(f"------ FROM {start_str} TO {end_str}")
    tweets_df = get_tweets(source_names, start_str, end_str)
    write_out_tweets(tweets_df, f"./output/most_liked3/{year}-W{week_num:02d}.csv")
    

def main():
    print("\n\n")
    
    if (len(sys.argv) < 2):
        print("NOT ENOUGH ARGUMENTS. PLEASE RUN THIS AS FOLLOWS:")
        print("python most_liked.py SOURCE_FILE WEEK_NUM")
        print("\n")
        print("SOURCEFILE - text file with one username per line")
        print("WEEK_NUM - number of week in current year")
        return
    
    source_filename = sys.argv[1]
    week_num = int(sys.argv[2])
    source_names = get_usernames(source_filename)
    get_tweets_for_week(source_names, date.today(), week_num)


if __name__ == "__main__":
    main()
import sys, os
from datetime import datetime, date, timedelta

from classes.patient_api_client import PatientApiClient
import pandas as pd
import csv


def get_user_ids():
    source_filename = sys.argv[1]
    return open(source_filename).read().splitlines()


# def get_tweets(api_client, source_ids, week_date):
#     start_time = get_datetime_iso(week_date, get_last_week)
#     end_time = get_datetime_iso(week_date, get_week)
#     return get_tweets(api_client, source_ids, start_time, end_time)


def get_tweets(api_client, source_ids, start_time, end_time):
    params = {
        "max_results": "100",
        "exclude": ["replies"],
        "tweet.fields": ["public_metrics,created_at"],
        "user.fields":["username"],
        "expansions":['author_id'],
        "end_time": end_time,
        "start_time": start_time
    }    
    df = pd.DataFrame(columns=['user_id', 'username', 'tweet_id', 'tweet_content', 'created_at', 'like_count', 'impression_count', 'quote_count', 'reply_count', 'retweet_count'])
    for user_id in source_ids:
        print(f"[{user_id}]] Getting tweets...")
        tweets = api_client.handle_request_json('tweets', f'users/:{user_id}/tweets', params)
        if "error" in tweets:
            print(f"[{user_id}] ERROR: {tweets['error']}")
            continue
        if "errors" in tweets:
            print(f"[{user_id}] ERROR: {tweets['errors']}")
            continue
        if "data" not in tweets:
            print(f"[{user_id}] NO DATA FOUND: {tweets}")
            continue
        username = tweets['includes']['users'][0]['username']
        if "meta" in tweets:
            print(f"[{user_id}] ({username}) result count: {tweets['meta']['result_count']}")
        else:
            print(f"[{user_id}] ({username}) NO METADATA")
        data_rows = []
        for tweet in tweets['data']:
            tweet_created_at = tweet['created_at']
            created_at_datetime = datetime.fromisoformat(tweet_created_at.replace('Z', '+00:00'))
            new_row = {'user_id': user_id,
                       'username': username,
                       'tweet_id': tweet['id'],
                       'tweet_content': tweet['text'],
                       'created_at': str(created_at_datetime.astimezone()),
                       'like_count': tweet["public_metrics"]['like_count'],
                       'impression_count': tweet["public_metrics"]['impression_count'],
                       'quote_count': tweet["public_metrics"]['quote_count'],
                       'reply_count': tweet["public_metrics"]['reply_count'],
                       'retweet_count': tweet["public_metrics"]['retweet_count']
                       }
            data_rows.append(new_row)
        df = pd.concat([df, pd.DataFrame.from_records(data_rows)])
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


def get_tweets_between_weeks(api_client, source_ids, end_date, start_week_num=1):
    if not os.path.exists("./output/most_liked3"):
        os.mkdir("./output/most_liked3")
    
    year, end_week_num, day_of_week = end_date.isocalendar()
    for week_num in range(start_week_num, end_week_num):
        if week_num != 16:
            continue
        start = datetime.strptime(f"{year}-W{week_num}" + '-1', "%Y-W%W-%w")
        start_str = 'T'.join(str(start.astimezone()).split())
        end = datetime.strptime(f"{year}-W{week_num+1}" + '-1', "%Y-W%W-%w")
        end_str = 'T'.join(str(end.astimezone()).split())
        print(f"------ GETTING RESULTS FOR WEEK {week_num} -------")
        print(f"------ FROM {start_str} TO {end_str}")
        tweets_df = get_tweets(api_client, source_ids, start_str, end_str)
        write_out_tweets(tweets_df, f"./output/most_liked3/{year}-W{week_num:02d}.csv")
    

def main():
    source_ids = get_user_ids()
    api_client = PatientApiClient()
    
    week_count = date.today().isocalendar()[1]
    source_ids_count = len(source_ids)
    print(f"NUMBER OF ACCOUNTS: {source_ids_count}")
    print(f"NUMBER OF WEEKS: {week_count}")
    print(f"NUMBER OF REQUESTS: {source_ids_count * week_count}")
    print(f"ESTIMATED RUNNING TIME: {(source_ids_count * week_count // 1500) * 16} MINUTES")
    
    user_input = input("Write [y] if you'd like to continue: ")
    if user_input.lower() != 'y':
        print(" -- ABORTING --")
        return

    get_tweets_between_weeks(api_client, source_ids, date.today(), start_week_num=1)


if __name__ == "__main__":
    main()
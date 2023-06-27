import os, sys
import pandas as pd

import twint.run
from twint import Config, run


def get_lookup_config(username):
    config = Config()
    config.User_id = username
    config.Store_csv = True
    config.Output = f"data/output/tweets/war-outbreak.csv"
    config.Utc = False
    config.Since = "2022-02-24 00:00:00"
    config.Until = "2022-03-25 00:00:00"
    config.Impressions = True
    return config


def load_user_data(filename: str):
    return pd.read_csv(filename)


def get_user_tweets(username):
    config = get_lookup_config(username)
    try:
        twint.run.Search(config)
    except Exception as e:
        print(e)


def get_tweets(source_data):
    for index, row in source_data.iterrows():
        if row['group'] == "CONTROL":
            continue
        # print(row)
        username: str = row['output_username']
        print(f"Getting tweets for user: {username}")
        get_user_tweets(username)

def set_is_reply(row):
    row['is_reply'] = row['reply_to'] != "[]"
    return row
    
    
def create_no_content_version():
    df = pd.read_csv("data/output/tweets/tweets.csv", low_memory=False)
    df = df.apply(set_is_reply, axis=1)
    allowed_columns = ["id", "conversation_id", "created_at", "date", "user_id", "username", "replies_count", "retweets_count", "likes_count", "quotes_count", "impressions_count", "is_reply"]
    df = df.loc[:, allowed_columns]
    
    df.to_csv("data/output/tweets/tweets_data.csv", index=False)


def set_month(row):
    row['month'] = row['created_at'][:7]
    return row


def get_user_table():
    df = pd.read_csv('data/updated_input.csv').query('output_user_id.notnull()',engine='python')
    df['output_user_id'] = df['output_user_id'].astype(int)
    df['blocked_date'] = df['blocked_date'].fillna('')
    df['note'] = df['note'].fillna('')
    
    test_find = df.query('output_user_id == 1239302803832668161')
    print("TEST FIND USER")
    print(test_find.head())
    
    allowed_columns = ['output_user_id', 'output_username', 'output_follower_count', 'group', 'blocked_date']
    df = df.loc[:, allowed_columns]
    df = df.rename(columns={'output_user_id': 'user_id'})
    df['user_id'] = df['user_id'].astype(int)
    return df
    

def aggregate():
    df = pd.read_csv(sys.argv[1], low_memory=False)
    df = df.apply(set_month, axis=1)
    
    agg_df = df.groupby(['user_id', 'month', 'is_reply']).agg({
        'id': ['count'],
        'replies_count': ['sum'],
        'retweets_count': ['sum'],
        'likes_count': ['sum'],
        'quotes_count': ['sum'],
        'impressions_count': ['sum']
    })

    print("\n\ngroupby AGG_DF\n\n")
    print(agg_df.head())

    # combine the column names of the multi-level index into a single string and set it as the column index
    agg_df.columns = ['_'.join(col).strip() for col in agg_df.columns.values]

    # reset the index and set the column headers
    agg_df = agg_df.reset_index()
    agg_df.columns = ['user_id', 'month', 'is_reply', 'count', 'replies_count', 'retweets_count', 'likes_count', 'quotes_count', 'impressions_count']
    print("\n\nflattened AGG_DF\n\n")
    print(agg_df.head())
    
    agg_df.to_csv('data/output/tweets/aggregated.csv', index=False)


def join():
    user_table = get_user_table()
    aggregated = pd.read_csv('data/output/tweets/aggregated.csv')
    aggregated['user_id'] = aggregated['user_id'].astype(int)
    print("__user table__")
    print(user_table.head(25))
    print("__aggregate__")
    print(aggregated.head(25))
    # joined = pd.merge(aggregated, user_table, left_on="user_id", right_on="output_user_id", how="left")
    joined = aggregated.join(user_table, on='user_id', rsuffix='user_')
    print("\n\nJOINED\n\n")
    
    print(joined.head(25))

    joined.to_csv('data/output/tweets/joined.csv', index=False)

def main():
    source_file = sys.argv[1]
    data = load_user_data(source_file).query("output_username.notnull()", engine='python')
    get_tweets(data)
    # create_no_content_version()
    # aggregate()
    # join()


if __name__ == '__main__':
    main()

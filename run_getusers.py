import os, sys, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from patient_api_client import PatientApiClient

import twint.run
import twint.storage
from twint import Config, run

def get_lookup_config(username):
    config = Config()
    config.User_id = username
    config.Followers = True
    config.Pandas = True
    return config

def apply_values(row, df):
    row['output_follower_count'] = df.loc[0, ['followers']].item()
    row['output_user_id'] = df.loc[0, ['id']].item()
    row['output_username'] = df.loc[0, ['username']].item()
    return row


def get_username(api: PatientApiClient, user_id):
    print(f"\nGetting username for twitter user id: {user_id}")
    response = api.handle_request_json(request_type="username",
                                       request=f'users/:{user_id}')
    if response.get("errors"):
        print("    Could not retrieve username. Please confirm account with this id still exists.")
        return None
    username: str = response.get("data").get("username")
    print(f"    Retrieved username: {username}")
    return username

def get_user_data(row, api):
    config = get_lookup_config(row['input_username'])
    # noinspection PyBroadException
    try:
        twint.run.Lookup(config)
        df = twint.storage.panda.User_df
        row = apply_values(row, df)
        return row
    except Exception as e:
        if str(e) == "Could not find the Guest token in HTML":
            print("Reached guest token limit, sleeping for 5 mins...")
            time.sleep(60 * 5)
            try:
                twint.run.Lookup(config)
                df = twint.storage.panda.User_df
                row = apply_values(row, df)
                return row
            except Exception as after_sleep_e:
                row['note'] = str(row['note']) + str(after_sleep_e) + "\n"
        else:
            row['note'] = str(row['note']) + str(e) + "\n"
        if row['input_user_id'] != 0:
            username = get_username(api, row['input_user_id'])
            if username is None:
                row['note'] = row['note'] + 'Could not get username from id\n'
                return row
            config = get_lookup_config(username)
            try:
                twint.run.Lookup(config)
                df = twint.storage.panda.User_df
                row = apply_values(row, df)
                return row
            except Exception as inner_e:
                row['note'] = str(row['note']) + str(inner_e) + "\n"
                return row


def get_most_recent_user_data(data):
    api = PatientApiClient()
    data = data.apply(get_user_data, args=(api,), axis=1)
    return data

def load_from_csv(filename: str):
    col_names = ['input_user_id', 'input_username', 'group', 'blocked_date', 'input_follower_count', 'output_user_id', 'output_username', 'output_follower_count', 'note']
    col_types = {'input_user_id': str, 'input_username': str, 'group': str, 'blocked_date': str, 'follower_count': 'Int64', 'output_user_id': str, 'output_username': str, 'output_follower_count': 'Int64', 'note': str}
    return pd.read_csv(filename, header=0, names=col_names, dtype=col_types, error_bad_lines=False)

def save_updated_user_db(data):
    if not os.path.exists('data/output/'):
        os.mkdir('data/output/')
    data.to_csv('data/output/updated_input_db.csv', index=False)

def main():
    source_file = sys.argv[1]

    data = load_from_csv(source_file)
    data = get_most_recent_user_data(data)
    save_updated_user_db(data)

def get_user_custom():
    source_names = []
    output = ""
    with open("source_names.txt", "r") as f:
        source_names = f.readlines()
        
    with open("output_users.csv", "a") as f:
        for source_name in source_names:
            username = source_name.strip()
            config = get_lookup_config(username)
            try:
                twint.run.Lookup(config)
                df = twint.storage.panda.User_df
                newl = f'{df["id"].iloc[0]},{df["name"].iloc[0]},"{username}",{df["tweets"].iloc[0]},{df["join_date"].iloc[0]},{df["avatar"].iloc[0]},{df["followers"].iloc[0]},{df["following"].iloc[0]}'
                output = newl + '\n'
                f.write(output)
                print(newl)
            except Exception as e:
                print(e)
            time.sleep(3)

if __name__ == '__main__':
    get_user_custom()
    # main()
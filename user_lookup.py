import os, sys, time

import twint.run
import twint.storage
from twint import Config


def get_lookup_config(username):
    config = Config()
    config.User_id = username
    config.Followers = True
    config.Pandas = True
    return config


def get_users_info(sourcefile: str, outputfile: str):
    source_names = []
    with open(sourcefile, "r") as f:
        source_names = [name.strip() for name in f.readlines()]

    if not os.path.exists(outputfile):
        with open(outputfile, "w+") as f:
            f.write(f'id,name,username,tweets,join_date,following,followers,avatar')

    with open(outputfile, "a", encoding='utf-8') as f:
        for source_name in source_names:
            config = get_lookup_config(source_name)
            try:
                twint.run.Lookup(config)
                df = twint.storage.panda.User_df
                output = f'{df["id"].iloc[0]},"{df["name"].iloc[0]}",{source_name},{df["tweets"].iloc[0]},{df["join_date"].iloc[0]},{df["following"].iloc[0]},{df["followers"].iloc[0]},{df["avatar"].iloc[0]}'
                f.write('\n' + output)
            except Exception as e:
                print(e)
            # 3 seconds between requests seems to be enough 
            # to prevent twitter from blocking the twint guest token
            time.sleep(3)


def main():
    if len(sys.argv) < 3:
        print("\n\n\nSome arguments may be missing, please run the script like so:")
        print("python user_lookup.py SOURCEFILE OUTPUTFILE")
        print("SOURCEFILE should be any text file with a list of usernames, with each username on a separate line\n\n\n")
        return

    if not os.path.exists(sys.argv[1]):
        print(f"\n\n\nSource file \"{sys.argv[1]}\" does not exists.\n\n\n")
        return

    get_users_info(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
    main()

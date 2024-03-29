# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    read_raw_data3.py                                  :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/11 11:16:25 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/05 16:02:21 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

demo_files = set([
    "Bernie Sanders",
    "SenSanders",
    "Joe Biden",
    "JoeBiden",
    #"Mike Bloomberg",
    #"MikeBloomberg",
    #"Tulsi Gabbard",
    #"TulsiGabbard",
    #"Elizabeth Warren",
    #"ewarren",
    #"Amy Klobuchar",
    #"amyklobuchar",  
    #"Pete Buttigieg",
    #"PeteButtigieg",
    # "Mayor Pete",
    # "Pete",
    # "Buttigieg",
    # "Tom Steyer",
    # "TomSteyer",
    # "Michael Bennet",
    # "SenatorBennet",
    # "Andrew Yang",
    # "AndrewYang",
    # "Deval Patrick",
    # "DevalPatrick",
    # "John Delaney",
    # "JohnDelaney",
])

from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm


def read_tweets_json(start, end):

    months = set([
        "202003",
        # "202002",
        # "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1
                    yield d, dt


def read_user_profile(start, end, set_users_before=None):

    months = set([
        "202003",
        # "202002",
        # "202001",
        # "201912",
        # "201911",
        # "201910",
        # "201909",
    ])

    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())
                    u = d["user"]
                    user_id = u["id"]
                    if "location" not in u:
                        continue
                    if user_id in set_users:
                        continue
                    set_users.add(user_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 1000 == 0:
                        print("New user ->", cnt)
                    cnt += 1
                    yield u


def write_fast_raw_data(start, end):
    months = set([
        "202003",
        "202002",
    ])

    out_file = open(f"data/fast_raw_tweets/{start.to_date_string()}-{end.to_date_string()}.lj", "w")
    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0
            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())

                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1

                    if "retweeted_status" in d and d["text"].startswith("RT @"):
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["retweeted_status"]["hashtags"],
                            "id": d["id"],
                            "user": {
                                "id": d["user"]["id"],
                                "screen_name": d["user"]["screen_name"],
                            },
                            "source": d["source"],
                            "text": d["retweeted_status"]["full_text"]
                        }

                    else:
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["hashtags"],
                            "id": d["id"],
                            "user": {
                                "id": d["user"]["id"],
                                "screen_name": d["user"]["screen_name"],
                            },
                            "source": d["source"],
                            "text": d["text"]
                        }

                    out_file.write(json.dumps(d, ensure_ascii=False) + "\n")


def write_fast_raw_data_v2(start, end):
    months = set([
        "201909",
        "201910",
        "201911",
        "201912",
        "202001",
        "202002",
        "202003",
    ])
    out_file = open(f"data/fast_raw_data_afterBT_all/{start.to_date_string()}-{end.to_date_string()}.lj", "w")
    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0
            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    d = json.loads(line.strip())

                    tweet_id = d["id"]
                    if tweet_id in set_tweets:
                        continue
                    set_tweets.add(tweet_id)

                    dt = pendulum.from_format(
                        d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                    if dt < start:
                        print("sum:", cnt, d["created_at"], "end!")
                        break
                    if dt >= end:
                        continue

                    if cnt % 50000 == 0:
                        print("New data ->", cnt)
                    cnt += 1

                    u = d["user"]
                    if "location" in u:
                        _u = {
                            "id": u["id"],
                            "screen_name": u["screen_name"],
                            "location": u["location"],
                            "profile_image_url": u["profile_image_url"],
                        }
                    else:
                        _u = {
                            "id": u["id"],
                            "screen_name": u["screen_name"],
                        }

                    if "retweeted_status" in d and d["text"].startswith("RT @"):
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["retweeted_status"]["hashtags"],
                            "id": d["id"],
                            "user": _u,
                            "source": d["source"],
                            "text": d["retweeted_status"]["full_text"]
                        }
                    else:
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["hashtags"],
                            "id": d["id"],
                            "user": _u,
                            "source": d["source"],
                            "text": d["text"]
                        }
                    out_file.write(json.dumps(d, ensure_ascii=False) + "\n")


def read_tweets_json_fast():
    for line in tqdm(open("/media/zhen/fast_raw_tweets_after_BT_4Q/2020-03-06-2020-03-29.lj")):
        d = json.loads(line.strip())
        dt = pendulum.from_format(
            d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
        yield d, dt


def read_tweets_json_fast_v2(start, end):
    for line in tqdm(open("data/fast_raw_data_afterBT_all/2019-09-01-2020-03-08.lj")):
        d = json.loads(line.strip())
        dt = pendulum.from_format(
            d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
        if dt >= start and dt <= end:
            yield d, dt


def read_user_profile_fast(set_users_before=None):
    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    in_names = [
        "data/fast_raw_tweets/2019-11-01-2020-01-01.lj",
        "data/fast_raw_tweets/2019-09-01-2019-11-01.lj",
        "data/fast_raw_tweets/2020-01-01-2020-02-24.lj",
    ]
    print('before:', len(set_users))
    for in_name in in_names:
        for line in tqdm(open(in_name)):
            u = json.loads(line.strip())["user"]
            user_id = u["id"]
            if user_id in set_users:
                continue
            set_users.add(user_id)
            if "location" not in u:
                continue
            yield u


if __name__ == '__main__':
     start = pendulum.datetime(2019, 9, 1 , tz="UTC")
     end = pendulum.datetime(2020, 3, 8, tz="UTC")
     write_fast_raw_data_v2(start, end)
#    count_tweets_users()

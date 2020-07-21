# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    read_raw_data.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/11 11:16:25 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/16 17:31:06 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

demo_files = set([
    "Bernie Sanders",
    "SenSanders",
    "Joe Biden",
    "JoeBiden",
    ##"Mike Bloomberg",
    ##"MikeBloomberg",
    ##"Tulsi Gabbard",
    ##"TulsiGabbard",
    ##"Elizabeth Warren",
    ##"ewarren",
    ##"Amy Klobuchar",
    ##"amyklobuchar",  
    ##"Pete Buttigieg",
    ##"PeteButtigieg",
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


election_files = set([
    "Biden"
    "Trump",
    "Joe Biden",
    "JoeBiden",
    "Donald Trump",
    "realDonaldTrump"
    "Trump OR Biden",
    "biden OR joebiden",
    "trump OR donaldtrump OR realdonaldtrump",
])

from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm


def read_historical_tweets(start, end):
    months = set([
        # "202001",
        # "202002",
        # "202003",
        # "202004",
        # "202005",
        # "202006",
        "202007",
    ])

    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in election_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0
            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end of the file!")
                        print("-" * 50)
                        break
    
                    try:
                        d = json.loads(line.strip())
                    except:
                        print('json.loads Error:', line)
                        continue

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


def read_raw_tweets_fromlj():
    set_tweetid = set()
    months = ["202001", "202002", "202003", "202004", "202005", "202006"]
    for month in months:
        print(month)
        for line in tqdm(open(f"/media/alex/data/US2020_raw/{month}.lj")):
            try:
                d = json.loads(line.strip())
            except:
                print('json.loads Error:', line)
                continue
            if d['id'] in set_tweetid:
                continue
            set_tweetid.add(d['id'])
            dt = pendulum.from_format(d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
            yield d, dt


def read_tweets_json(start, end):

    months = set([
        "202004",
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


def read_raw_user_month(month, _set_users):
    # 只保留有location信息的
    for line in open(f"disk/raw_data/{month}.lj"):
        u = json.loads(line.strip())["user"]
        user_id = u["id"]
        if user_id in _set_users:
            continue
        _set_users.add(user_id)
        if "location" in u:
            yield u


def read_raw_data_month(month, _set_tweet_ids):
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
    for in_name in file_names:
        word = in_name.stem.split("-")[-1].lower()
        if ("biden" in word or "trump" in word) and in_name.parts[1] == month:
            print(in_name)
            for line in open(in_name):
                d = json.loads(line.strip())
                if d["id"] in _set_tweet_ids:
                    continue
                _set_tweet_ids.add(d["id"])
                yield d


def read_raw_user(start, end, set_users_before=None):

    months = set([
        "202006",
    ])

    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in election_files and in_name.parts[1] in months:
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

                    if cnt % 2000 == 0:
                        print("New user ->", cnt)
                    cnt += 1
                    yield u

def count_tweets_users():
    
    set_tweets = set()
    set_users = set()

    months = set([
        "202003",
        "202002",
        "202001",
        "201912",
        "201911",
        "201910",
        "201909",
    ])

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        if in_name.stem.split("-")[-1] in demo_files and in_name.parts[1] in months:
            print(in_name)
            cnt = 0

            for line in open(in_name):
                d = json.loads(line.strip())
                tid = d["id"]
                uid = d["user"]["id"]
                set_tweets.add(tid)
                set_users.add(uid)
            
            print(len(set_tweets), len(set_users))


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
                            "user": d["user"],
                            "source": d["source"],
                            "text": d["retweeted_status"]["full_text"]
                        }

                    else:
                        d = {
                            "created_at": d["created_at"],
                            "hashtags": d["hashtags"],
                            "id": d["id"],
                            "user": d["user"],
                            "source": d["source"],
                            "text": d["text"]
                        }

                    out_file.write(json.dumps(d, ensure_ascii=False) + "\n")


def read_tweets_json_fast():
    for line in tqdm(open("/media/zhen/fast_raw_tweets_after_BT_4Q/2019-09-01-2020-03-06.lj")):
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


def read_users_set():
    months = ["202001", "202002", "202003", "202004", "202005"]
    set_users = set()
    for month in months:
        for line in open("disk/users-profile/" + month + ".lj"):
            u = json.loads(line.strip())
            set_users.add(u["id"])
    print("Number of the users:", len(set_users))
    return set_users


if __name__ == '__main__':

    # 组合新的原始数据
    _set_tweetid = set()
    _set_users = set()
    months = ["202001", "202002", "202003", "202004", "202005", "202006"]
    
    for month in months:
        f_data = open(f"/media/alex/data/US2020_raw/{month}.lj", "w")
        f_user = open(f"disk/users-profile/{month}.lj", "w")
        data_iter = read_raw_data_month(month, _set_tweetid)
        for d in data_iter:
            f_data.write(json.dumps(d, ensure_ascii=False) + "\n")
            u = d["user"]
            if "location" not in u or u["id"] in _set_users:
                continue
            u = {
                'id': u['id'],
                'screen_name': u['screen_name'],
                'location': u['location']
            }
            f_user.write(json.dumps(u, ensure_ascii=False) + "\n")
            _set_users.add(u["id"])
        f_data.close()
        f_user.close()



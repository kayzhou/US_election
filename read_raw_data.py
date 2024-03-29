# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    read_raw_data.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/11 11:16:25 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/11/26 09:52:24 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm

# add trump
demo_files = set([
    # "Bernie Sanders",
    # "SenSanders",
    "Joe Biden",
    "JoeBiden",
    # "Mike Bloomberg",
    # "MikeBloomberg",
    # "Tulsi Gabbard",
    # "TulsiGabbard",
    # "Elizabeth Warren",
    # "ewarren",
    # "Amy Klobuchar",
    # "amyklobuchar",  
    # "Pete Buttigieg",
    # "PeteButtigieg",
    # "John Delaney",
    # "JohnDelaney",
    # "Deval Patrick",
    # "DevalPatrick",
    # "Tom Steyer",
    # "TomSteyer",
    # "Andrew Yang",
    # "AndrewYang",
    "Donald Trump",
    "realDonaldTrump",
])


election_files = set([
    # "Biden"
    # "Trump",
    # "Joe Biden",
    # "JoeBiden",
    # "Donald Trump",
    # "realDonaldTrump"
    # "Trump OR Biden",
    "biden OR joebiden",
    "trump OR donaldtrump OR realdonaldtrump",
])


def union_all_data():
    """合并9、10月数据
    去重
    """
    # from pathlib import Path

    start = pendulum.date(2020, 9, 1)
    end = pendulum.date(2020, 9, 30)
    with open("D:/US2020/202009.lj", "w", encoding="utf8") as f:
        for dt in pendulum.period(start, end):
            set_tweetid = set()
            print(dt.format("YYYYMMDD"))
            for line in open(f"D:/US2020/202009/{dt.format('YYYYMMDD')}-biden OR joebiden.txt", encoding='utf8'):
                try:
                    _id = json.loads(line)['id']
                    if _id not in set_tweetid:
                        f.write(line)
                        set_tweetid.add(_id)
                except:
                    print("ERROR")
            for line in open(f"D:/US2020/202009/{dt.format('YYYYMMDD')}-trump OR donaldtrump OR realdonaldtrump.txt", encoding='utf8'):
                try:
                    _id = json.loads(line)['id']
                    if _id not in set_tweetid:
                        f.write(line)
                        set_tweetid.add(_id)
                except:
                    print("ERROR")

    start = pendulum.date(2020, 10, 1)
    end = pendulum.date(2020, 10, 31)
    with open("D:/US2020/202010.lj", "w", encoding="utf8") as f:
        for dt in pendulum.period(start, end):
            set_tweetid = set()
            print(dt.format("YYYYMMDD"))
            for line in open(f"D:/US2020/202010/{dt.format('YYYYMMDD')}-biden OR joebiden.txt", encoding='utf8'):
                try:
                    _id = json.loads(line)['id']
                    if _id not in set_tweetid:
                        f.write(line)
                        set_tweetid.add(_id)
                except:
                    print("ERROR")
            for line in open(f"D:/US2020/202010/{dt.format('YYYYMMDD')}-trump OR donaldtrump OR realdonaldtrump.txt", encoding='utf8'):
                try:
                    _id = json.loads(line)['id']
                    if _id not in set_tweetid:
                        f.write(line)
                        set_tweetid.add(_id)
                except:
                    print("ERROR")


def read_historical_tweets(start, end):
    months = set([
        "202001",
        "202002",
        "202003",
        "202004",
        "202005",
        "202006",
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
                    except Exception:
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


def read_month_raw_tweets_fromlj(month):
    """直接读取raw_data/month.lj文件
    Yields:
        [type]: [description]
    """
    for line in open(f"D:/US2020/{month}.lj", encoding="utf8"):
        try:
            d = json.loads(line.strip())
        except Exception as e:
            print('json.loads() Error:', e)
            print('line ->', line)
            continue
        dt = pendulum.from_format(d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
        # 时差问题
        dt = dt.add(hours=-4)
        yield d, dt


def read_raw_tweets_fromlj(_month="all"):
    """直接读取raw_data/month.lj文件
    Yields:
        [type]: [description]
    """
    if _month == "all":
        months = ["202008", "202007", "202006", "202005", "202004", "202003"]
        for month in months:
            set_tweetid = set()
            print(month)
            for line in open(f"raw_data/raw_data/{month}.lj"):
                try:
                    d = json.loads(line.strip())
                except Exception as e:
                    print('json.loads() Error:', e)
                    print('line ->', line)
                    continue
                if d['id'] in set_tweetid:
                    continue
                set_tweetid.add(d['id'])
                dt = pendulum.from_format(d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')
                # 时差问题
                dt = dt.add(hours=-4)
                yield d, dt
    else:
        set_tweetid = set()
        print(_month)
        if _month in ["202004", "202005", "202006"]:
            in_name = f"/media/alex/data/US2020_raw/{_month}.lj"
        else:
            in_name = f"raw_data/raw_data/{_month}.lj"
        for line in open(in_name):
        # for line in tqdm(open(f"/external2/zhenkun/US_election_data/raw_data/{month}.lj")):
            try:
                d = json.loads(line.strip())
            except Exception as e:
                print('json.loads Error:', e)
                print('line ->', line)
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


def read_tweets_json_day(dt):
    print("read_tweets_json_day:", dt.to_date_string())
    set_tweets = set()
    dt_str = dt.format('YYYYMMDD')
    file_names = Path(f"raw_data/{dt.format('YYYYMM')}").rglob(f"*.txt")

    for in_name in file_names:
        if not in_name.parts[-1].startswith(dt_str):
            continue

        print(in_name)
        cnt = 0
        for line in open(in_name):
            try:
                d = json.loads(line.strip())
            except Exception:
                print("ERROR: json.loads()")
                continue
            tweet_id = d["id"]
            if tweet_id in set_tweets:
                continue
            set_tweets.add(tweet_id)

            t_dt = pendulum.from_format(
                d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')

            if cnt % 50000 == 0:
                print("New data ->", cnt)
            cnt += 1
            yield d, t_dt


def count_tweets_json_day(dt):
    print("read_tweets_json_day:", dt.to_date_string())
    dt_str = dt.format('YYYYMMDD')

    file_name = f"raw_data/{dt.format('YYYYMM')}/{dt_str}-biden OR joebiden.txt"
    count_B = 0
    for _, line in enumerate(open(file_name)):
        count_B += 1

    file_name = f"raw_data/{dt.format('YYYYMM')}/{dt_str}-trump OR donaldtrump OR realdonaldtrump.txt"
    count_T = 0
    for _, line in enumerate(open(file_name)):
        count_T += 1

    return count_B, count_T


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


def read_raw_data_month_Jan_to_March(month):
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
    set_tweets = set()
    for in_name in file_names:
        word = in_name.stem.split("-")[-1]
        if word in demo_files and in_name.parts[1] == month:
            print(in_name)
            for line in open(in_name):
                try:
                    d = json.loads(line.strip())
                except Exception:
                    print("JSON loading error")
                if d["id"] in set_tweets:
                    continue
                set_tweets.add(d["id"])

                d["query"] = word
                
                t_dt = pendulum.from_format(
                    d["created_at"], 'ddd MMM DD HH:mm:ss ZZ YYYY')

                yield d, t_dt


def read_raw_data_month(month, _set_tweet_ids):
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
    for in_name in file_names:
        word = in_name.stem.split("-")[-1].lower()
        if ("biden" in word or "trump" in word) and in_name.parts[1] == month:
            print(in_name)
            for line in open(in_name):
                try:
                    d = json.loads(line.strip())
                except Exception:
                    print("JSON loading error")
                if d["id"] in _set_tweet_ids:
                    continue
                _set_tweet_ids.add(d["id"])
                yield d


def read_raw_user(start, end, set_users_before=None):

    months = set([
        "202007",
    ])

    if set_users_before:
        set_users = set_users_before
    else:
        set_users = set()

    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        word = in_name.stem.split("-")[-1].lower()
        if ("biden" in word or "trump" in word) and in_name.parts[1] in months:
            print(in_name)
            cnt = 0
            with FileReadBackwards(in_name) as f:
                while True:
                    line = f.readline()
                    if not line:
                        print(cnt, "end!")
                        print("-" * 50)
                        break

                    try:
                        d = json.loads(line.strip())
                    except:
                        print("ERROR line")
                        continue
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

    union_all_data()
    # 组合新的原始数据
    # _set_users = set()
    # months = ["202001", "202002", "202003", "202004", "202005", "202006"]
    # months = ["202008"]
    
    # for month in months:
    #     _set_tweetid = set()
    #     f_data = open(f"/external1/zhenkun/raw_data/{month}.lj", "w")
    #     # f_user = open(f"disk/users-profile/{month}.lj", "w")
    #     data_iter = read_raw_data_month(month, _set_tweetid)
    #     for d in data_iter:
    #         try:
    #             f_data.write(json.dumps(d, ensure_ascii=False) + "\n")
    #         except Exception:
    #             print("JSON dump error.")
            # u = d["user"]
            # if "location" not in u or u["id"] in _set_users:
            #     continue
            # u = {
            #     'id': u['id'],
            #     'screen_name': u['screen_name'],
            #     'location': u['location']
            # }
            # f_user.write(json.dumps(u, ensure_ascii=False) + "\n")
            # _set_users.add(u["id"])
        # f_data.close()
        # f_user.close()

    # pass


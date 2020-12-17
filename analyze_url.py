# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_url.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/12/17 14:02:36 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from tqdm import tqdm
from collections import Counter
from file_read_backwards import FileReadBackwards


election_files = set([
    "biden OR joebiden",
    "trump OR donaldtrump OR realdonaldtrump",
])

def read_tweets(start, end):
    months = set([
        # "202006",
        # "202007",
        "202008",
    ])
    set_tweets = set()
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)

    for in_name in file_names:
        query = in_name.stem.split("-")[-1]
        if ("biden" in query or "trump" in query) and in_name.parts[1] in months:
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
            yield d


def write_top_trump_biden_url(start, end, out_name):
    url_counter = Counter()
    for dt in pendulum.period(start, end):
        for d in read_tweets_json_day(dt):
            if d["urls"]:
                for url in d["urls"]:
                    url = url["expanded_url"]
                    if url.startswith("https://twitter.com"):
                        continue
                    url_counter[url] += 1
    with open(out_name, "w") as f:
        for ht, cnt in url_counter.most_common():
            print(ht, cnt, file=f)


if __name__ == "__main__":
    
    start = pendulum.datetime(2020, 11, 23, tz="UTC")
    end = pendulum.datetime(2020, 11, 30, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0824-0831.txt")

    start = pendulum.datetime(2020, 11, 16, tz="UTC")
    end = pendulum.datetime(2020, 11, 23, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0831-0907.txt")

    start = pendulum.datetime(2020, 11, 9, tz="UTC")
    end = pendulum.datetime(2020, 11, 16, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0907-0914.txt")

    start = pendulum.datetime(2020, 11, 2, tz="UTC")
    end = pendulum.datetime(2020, 11, 9, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0914-0921.txt")

    start = pendulum.datetime(2020, 10, 26, tz="UTC")
    end = pendulum.datetime(2020, 11, 2, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0921-0928.txt")

    start = pendulum.datetime(2020, 10, 19, tz="UTC")
    end = pendulum.datetime(2020, 10, 26, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0928-1005.txt")

    start = pendulum.datetime(2020, 10, 12, tz="UTC")
    end = pendulum.datetime(2020, 10, 19, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-1005-1012.txt")
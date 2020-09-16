# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_url.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/09/16 14:48:24 by Zhenkun          ###   ########.fr        #
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


def write_top_trump_biden_url(start, end, out_name):
    url_counter = Counter()
    for d, _ in read_tweets(start, end):
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
    start = pendulum.datetime(2020, 8, 3, tz="UTC")
    end = pendulum.datetime(2020, 8, 10, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0803-0810.txt")

    start = pendulum.datetime(2020, 8, 10, tz="UTC")
    end = pendulum.datetime(2020, 8, 17, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0810-0817.txt")
    
    start = pendulum.datetime(2020, 8, 17, tz="UTC")
    end = pendulum.datetime(2020, 8, 24, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0817-0824.txt")
    
    start = pendulum.datetime(2020, 8, 24, tz="UTC")
    end = pendulum.datetime(2020, 8, 31, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0824-0831.txt")

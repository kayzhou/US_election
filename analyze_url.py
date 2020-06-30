# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_url.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/22 09:43:13 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from tqdm import tqdm
from collections import Counter

trump_files = [
    "trump",
    "biden",
]
 
months = set([
    "202006",
    # "202005",
    # "202004",
    # "202003",
])


def write_top_trump_biden_url(start, end, out_name):
    url_counter = Counter()
    from read_raw_data import read_historical_tweets as read_tweets
    for d, dt in read_tweets(start, end):
        if d["urls"]:
            for url in d["urls"]:
                url = url["expanded_url"]
                url_counter[url] += 1
    with open(out_name, "w") as f:
        for ht, cnt in url_counter.most_common():
            print(ht, cnt, file=f)
            

if __name__ == "__main__":
    start = pendulum.datetime(2020, 6, 22, tz="UTC")
    end = pendulum.datetime(2020, 6, 29, tz="UTC")
    write_top_trump_biden_url(start, end, "data/url-0615-0622.txt")

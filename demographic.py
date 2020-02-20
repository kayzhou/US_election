# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    demographic.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/06/05 13:45:30 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/06/28 00:09:44 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import get_tweets_json
from geopy.geocoders import Nominatim, GoogleV3
from tqdm import tqdm
import time
import os
import ujson as json
# from pygeocoder import Geocoder

geolocator = Nominatim()

def get_tweets_json():
    set_tweets = set()
    bingo = False
    target_dir = ["201902", "201903", "201904", "201905", "201906"]
    # target_dir = ["201906"]
    for _dir in target_dir:
        for in_name in os.listdir("disk/" + _dir):
            if in_name.endswith("PRO.txt") or in_name.endswith("Moreno.txt") or in_name.endswith("Sola.txt"):
                continue
            print(in_name)
            in_name = "disk/" + _dir + "/" + in_name

            for line in open(in_name, encoding="utf-8"):
                d = json.loads(line.strip())
                tweet_id = d["id"]
                if tweet_id in set_tweets:
                    continue
                set_tweets.add(tweet_id)
                yield d, tweet_id

# with open("disk/data/outsides_0613.txt", "w") as f:
#     for d, t_id in tqdm(get_tweets_json()):
#         if "coordinates" in d:
#             try:
#                 # print(d["user"]["location"])
#                 coor = d["coordinates"]["coordinates"]
#                 # print(coor)
#                 f.write(f'{d["id"]}\t{d["user"]["id"]}\t{coor[1]},{coor[0]}\n')
#                 # location = geolocator.reverse(f"{coor[1]},{coor[0]}")
#                 # print(location.raw)

#                 # if location.raw['address']["country"] != "Argentina":
#                 #     print(f'{d["id"]}\t{d["user"]["id"]}\t{location.raw["address"]["country"]}')
                    
#                 # time.sleep(10)
#             except Exception as e:
#                 print(e)

# invalid user 196342735

with open("disk/data/outsides_country_0624.txt") as f:
    for line in f:
        go = line.strip().split("\t")[0]


bingo = False
with open("disk/data/outsides_country_0624.txt", "a") as f:
    for line in open("disk/data/outsides_0613.txt"):
        w = line.strip().split("\t")
        if w[0] == go:
            bingo = True
        
        if bingo:
            if w[1] == "196342735":
                continue
            coor = w[2].split(",")
            try:
                location = geolocator.reverse(f"{coor[0]},{coor[1]}")
                # print(location.raw)
                if location.raw['address']["country"] != "Argentina":
                    print(f'{w[0]}\t{w[1]}\t{location.raw["address"]["country"]}')
                    f.write(f'{w[0]}\t{w[1]}\t{location.raw["address"]["country"]}\n')
                # time.sleep(10)
            except Exception as e:
                print(e)

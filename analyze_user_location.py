# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user_location.py                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/19 19:49:03 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *

demo_files = set([
    "Michael Bennet",
    "SenatorBennet",
    "Joe Biden",
    "JoeBiden",
    "Mike Bloomberg",
    "MikeBloomberg",
    "Pete Buttigieg",
    "PeteButtigieg",
    "John Delaney",
    "JohnDelaney",
    "Tulsi Gabbard",
    "TulsiGabbard",
    "Amy Klobuchar",
    "amyklobuchar",
    "Deval Patrick",
    "DevalPatrick",
    "Bernie Sanders",
    "SenSanders",
    "Tom Steyer",
    "TomSteyer",
    "Elizabeth Warren",
    "ewarren",
    "Andrew Yang",
    "AndrewYang",
])

from pathlib import Path
from collections import Counter
from tqdm import tqdm


def write_locations():
    all_locations = Counter()
    set_users = set()

    # for in_name in Path("raw_data").rglob("*.txt"):
    #     if in_name.stem.split("-")[-1] in demo_files:
    #         print(in_name)
    #         for line in tqdm(open(in_name)):
    #             u = json.loads(line)["user"]
    #             if "location" in u:
    #                 all_locations[u["location"].lower().replace("\t", " ").replace("\n", " ")] += 1

    # with open("data/user-location-20200207.txt", "w") as f:
    #     for loc, cnt in all_locations.most_common(50000):
    #         print(loc, cnt, file=f, sep="\t")

    for line in tqdm(open("disk/02-15-user-profile.lj")):
        u = json.loads(line)
        _id = u["id"]
        if _id not in set_users:
            if "location" not in u:
                continue
            set_users.add(_id)
            if "location" in u:
                all_locations[u["location"].lower().replace("\t", " ").replace("\n", " ")] += 1

    with open("data/user-location-20200207.txt", "w") as f:
        for loc, cnt in all_locations.most_common(50000):
            print(loc, cnt, file=f, sep="\t")



def write_users_state():    
    set_users = set()
    loc_to_state = json.load(open("data/loc-to-state.json"))

    # with open("data/user-state-20200207.txt", "w") as f:
    #     for in_name in Path("raw_data").rglob("*.txt"):
    #         if in_name.stem.split("-")[-1] in demo_files:
    #             print(in_name)
    #             for line in tqdm(open(in_name)):
    #                 u = json.loads(line)["user"]
    #                 _id = u["id"]
    #                 if _id not in set_users:
    #                     if "location" not in u:
    #                         continue
    #                     set_users.add(_id)
    #                     location = u["location"].lower().replace("\t", " ").replace("\n", " ")
    #                     if location in loc_to_state:
    #                         state = loc_to_state[location]
    #                         name = u["screen_name"]
    #                         f.write(f"{_id},{name},{state}\n")


    with open("data/user-state-20200207.txt", "w") as f:
        for line in tqdm(open("disk/02-15-user-profile.lj")):
            u = json.loads(line)
            _id = u["id"]
            if _id not in set_users:
                if "location" not in u:
                    continue
                set_users.add(_id)
                location = u["location"].lower().replace("\t", " ").replace("\n", " ")
                if location in loc_to_state:
                    state = loc_to_state[location]
                    name = u["screen_name"]
                    f.write(f"{_id},{name},{state}\n")


def write_users_csv():
    # load json
    loc_to_state = json.load(open("data/loc-to-state.json"))
    loc_to_county = json.load(open("data/loc-to-county.json"))

    set_users = set()
    data = []

    for line in open("disk/02-15-user-profile.lj"):
        # print(line)
        u = json.loads(line)
        _id = u["id"]
        if _id not in set_users:
            if "location" not in u:
                continue
            set_users.add(_id)
            loc = u["location"].lower().replace("\t", " ").replace("\n", " ")

            state = None
            county = None
            if loc in loc_to_state:
                state = loc_to_state[loc]
            if loc in loc_to_county:
                county = loc_to_county[loc]

            # to_csv
            d = {
                "uid": _id,
                "name": u['screen_name'],
                "loc": loc,
                "state": state,
                "county": county
            }
            data.append(d)

    pd.DataFrame(data).set_index("uid").to_csv("disk/02-15-user-location.csv")


if __name__ == '__main__':
    # write_locations()
    write_users_csv()


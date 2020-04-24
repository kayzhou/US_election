# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user_location.py                           :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/04/24 19:04:15 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from pathlib import Path
from collections import Counter
from tqdm import tqdm


ef write_locations(in_name, out_name):
    # explain the locations > states and counties
    all_locations = Counter()
    set_users = set()

    for line in tqdm(open(in_name)):
        u = json.loads(line)
        _id = u["id"]
        if _id not in set_users:
            if "location" not in u:
                continue
            set_users.add(_id)
            if "location" in u:
                all_locations[u["location"].lower().replace("\t", " ").replace("\n", " ")] += 1

    with open(out_name, "w") as f:
        for loc, cnt in all_locations.most_common(1000):
            print(loc, cnt, file=f, sep="\t")d


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


def write_users_csv(in_name, out_name):
    """
    in_name: users-profile
    out_name: users-location.csv
    """

    # load json
    loc_to_state = json.load(open("data/loc-to-state.json"))
    loc_to_county = json.load(open("data/loc-to-county.json"))

    set_users = set()
    data = []

    for line in tqdm(open(in_name)):
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


            if state:
                if len(state) != 2:
                    print("Error: len(state) != 2")

                else:
                    d = {
                        "uid": _id,
                        # "name": u['screen_name'],
                        # "loc": loc,
                        "state": state,
                        "county": county
                    }
                    data.append(d)

    pd.DataFrame(data).set_index("uid").to_csv(out_name)


def write_users_today_csv(dt):
    """
    in_name: users-profile
    out_name: users-location.csv
    """
    dt_str = dt.to_date_string()

    # load json
    loc_to_state = json.load(open("data/loc-to-state.json"))
    loc_to_county = json.load(open("data/loc-to-county.json"))

    set_users = set()
    data = []

    for line in tqdm(open(f"disk/users-profile/{dt_str}.lj")):
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
                
            if state:
                if len(state) != 2:
                    print("Error: len(state) != 2")

                else:
                    d = {
                        "uid": _id,
                        # "name": u['screen_name'],
                        # "loc": loc,
                        "state": state,
                        "county": county
                    }
                    data.append(d)

    pd.DataFrame(data).set_index("uid").to_csv(f"disk/users-location/{dt_str}.csv")


if __name__ == '__main__':
    # more loc information should be mapped to state and county infomation.
    # write_locations("disk/users-profile/2020-02-24-2020-02-28.lj",
    #                 "disk/users-location/2020-02-24-2020-02-28-stat.txt")
    
    # write_users_csv("disk/users-profile/2020-02-29.lj",
    #                 "disk/users-location/2020-02-29.csv")

    dt = pendulum.datetime(2020, 3, 1, tz="UTC")
    # actually, we use today as the prediction 
    write_users_today_csv(dt)
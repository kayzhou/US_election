# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_reweight.py                             :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/05/03 09:01:29 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/10/28 17:35:41 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import json
import os

import pandas as pd
import ujson as json
import us  # USA address
from tqdm import tqdm

USA_ADDR_NAME = us.states.mapping('abbr', 'name')
USA_STATES = ['CA', 'TX', 'NY', 'FL', 'IL', 'GA',
    'PA', 'OH', 'DC', 'NC', 'MI', 'MA',
    'IN', 'NJ', 'VA', 'AZ', 'TN', 'WA',
    'MD', 'CO', 'MO', 'KY', 'LA', 'MN',
    'OR', 'AL', 'SC', 'NV', 'OK', 'WI',
    'IA', 'CT', 'KS', 'AR', 'UT', 'MS',
    'WV', 'NE', 'NM', 'HI', 'NH', 'RI',
    'ME', 'ID', 'AK', 'DE', 'MT', 'SD',
    'ND', 'VT', 'WY', 'USA'
]


# def load_users_opinion(in_name):
#     users = pd.read_csv(in_name).set_index("uid")
#     return users

def load_users_opinion(in_name):
    print("Loading users from:", in_name)
    if os.path.exists(in_name):
        users = json.load(open(in_name))
    else:
        print("Not exist")
        users = {}
    print("# of users:", len(users))

    users_list = []
    for uid, u in users.items():
        users_list.append({"uid": uid, "0": u[0], "1": u[1]})
    users = pd.DataFrame(users_list).set_index("uid")
    return users
    

def load_users_location(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    # print(users)
    # for line in open(in_name):
    #     w = line.strip()
    return users


def load_users_face(in_name):
    users = []
    print("loading_users_face()", in_name)
    for line in tqdm(open(in_name)):
        d = json.loads(line.strip())
        if not d["faces"] or len(d["faces"]) == 0:
            continue
        face = d["faces"][0]
        # print(face)
        age = face['attributes']["age"]["value"]
        gender = face['attributes']["gender"]["value"]

        if age < 18:
            continue
        elif age >= 18 and age < 30:
            age_range = ">=18, <30"
        elif age >= 30 and age < 50:
            age_range = ">=30, <50"
        elif age >= 50 and age < 65:
            age_range = ">=50, <65"
        else:
            age_range = ">=65"
        
        users.append({"uid": str(d["id"]), "age": age, "gender": gender, "age_range": age_range, "State": d["State"]})
            
    users = pd.DataFrame(users).set_index("uid")
    users = users[~users.index.duplicated(keep='first')]
    return users
  

def load_users_union():
    users = load_users_opinion("data/users-cumFrom01/2020-10-19.json")
    u2 = load_users_face("raw_data/user_info/Users_swing_info_final.lj")
    # u2 = load_users_location("raw_data/user_info/Users_swing_info_final.lj")
    # u3 = load_users_location("disk/users-face/2020-04-30.csv")
    users = users.join(u2, how="inner")
    # users = users.join(u2, how="inner").join(u3, how="inner")
    users["Camp"] = "None"
    users.loc[users["0"] >= users["1"], "Camp"] = "Biden"
    users.loc[users["0"] < users["1"], "Camp"] = "Trump"
    print(users)
    return users


def pred_per_state():
    users = load_users_union()
    rst = []
    for state_name in USA_STATES:
        users_tmp = users[users.State == state_name]
        groups = users_tmp.groupby(["Camp"]).size()
        print(state_name, groups)
        rst.append(
            {
                "State": USA_ADDR_NAME[state_name],
                "abbr": state_name,
                "Biden": groups.get("Biden", 0),
                "Trump": groups.get("Trump", 0),
            }
        )
    pd.DataFrame(rst).to_csv("data/csv/states-2020-10-19.csv")


def rescale_opinion(input_users, state_name):
    if state_name == "US":
        cen = pd.read_csv(f"data/census/US.csv").set_index("category")
    else:
        addr = USA_ADDR_NAME[state_name]
        cen = pd.read_csv(f"data/census/{addr}.csv").set_index("category")
    
    w = cen.percent.to_list()
    camps = [
         "Biden",
         "Trump",
    ]
    if state_name == "US":
        users_tmp = input_users
    else:
        users_tmp = input_users[input_users.State == state_name]
        
    groups = users_tmp.groupby(["age_range", "gender", "Camp"]).size()
    print(groups)
    _rst = {"Biden": 0, "Trump": 0}
    _analysis = {}

    # cross-classification weight table
    # first index: female -> 2, male -> 1
    # second index: ages from young to old
    try:
        G12 = sum(groups[">=18, <30"]["Female"])
        G11 = sum(groups[">=18, <30"]["Male"])
        G22 = sum(groups[">=30, <50"]["Female"])
        G21 = sum(groups[">=30, <50"]["Male"])
        G32 = sum(groups[">=50, <65"]["Female"])
        G31 = sum(groups[">=50, <65"]["Male"])
        G42 = sum(groups[">=65"]["Female"])
        G41 = sum(groups[">=65"]["Male"])
    except Exception:
        print("No enough groups.")
        return _rst, _analysis

    for _camp in camps:
        # T means the number of users in each group
        T12 = groups[">=18, <30"]["Female"].get(_camp, 0)
        T11 = groups[">=18, <30"]["Male"].get(_camp, 0)
        T22 = groups[">=30, <50"]["Female"].get(_camp, 0)
        T21 = groups[">=30, <50"]["Male"].get(_camp, 0)
        T32 = groups[">=50, <65"]["Female"].get(_camp, 0)
        T31 = groups[">=50, <65"]["Male"].get(_camp, 0)
        T42 = groups[">=65"]["Female"].get(_camp, 0)
        T41 = groups[">=65"]["Male"].get(_camp, 0)

        # each group r_jk = T_jk * (w_jk / G_jk)
        r = T12 / G12 * w[0] + T11 / G11 * w[1] \
            + T22 / G22 * w[2] + T21 / G21 * w[3] \
            + T32 / G32 * w[4] + T31 / G31 * w[5] \
            + T42 / G42 * w[6] + T41 / G41 * w[7] \

        _rst[_camp] = round(r * 100, 1)
        # _analysis return the number in each group
        _analysis[_camp] = [T12, T11, T22, T21, T32, T31, T42, T41]
    return _rst, _analysis


def rescale_per_state():
    rst = []
    input_users = load_users_union()
    for state_name in USA_STATES:
        _r, _ana = rescale_opinion(input_users, state_name)
        rst.append(
            {
                "State": USA_ADDR_NAME[state_name],
                "abbr": state_name,
                "Biden": _r["Biden"],
                "Trump": _r["Trump"],
            }
        )
    pd.DataFrame(rst).to_csv("data/csv/states-rescale-2020-10-19-onlyTB.csv")


if __name__ == "__main__":
    # pred_per_state()
    rescale_per_state()

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_reweight.py                             :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/05/03 09:01:29 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/18 17:11:17 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

import pandas as pd
import us # USA address

USA_ADDR_NAME = us.states.mapping('abbr', 'name')
USA_STATES = ['CA', 'TX', 'NY', 'FL', 'IL', 'GA',
  'PA', 'OH', 'DC', 'NC', 'MI', 'MA',
  'IN', 'NJ', 'VA', 'AZ', 'TN', 'WA',
  'MD', 'CO', 'MO', 'KY', 'LA', 'MN',
  'OR', 'AL', 'SC', 'NV', 'OK', 'WI',
  'IA', 'CT', 'KS', 'AR', 'UT', 'MS',
  'WV', 'NE', 'NM', 'HI', 'NH', 'RI',
  'ME', 'ID', 'AK', 'DE', 'MT', 'SD',
  'ND', 'VT', 'WY']


def load_users_opinion(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    return users
    

def load_users_location(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    print(users)
    return users


def load_users_face(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    return users
  

def load_users_union():
    users = load_users_opinion("disk/users-culFrom01/2020-04-30.csv")
    u2 = load_users_location("disk/users-location/2020-04-30.csv")
    users = users.join(u2, how="inner")
    users["Camp"] = "None"
    users.loc[users["0"] >= users["1"], "Camp"] = "Biden"
    users.loc[users["0"] < users["1"], "Camp"] = "Trump"
    print(users)
    return users


def pred_per_state():
    users = load_users_union()

    rst = []

    for state_name in USA_STATES:
        users_tmp = users[users.state == state_name]
        groups = users_tmp.groupby(["Camp"]).size()
        print(state_name, groups)
        rst.append(
            {
                "state": USA_ADDR_NAME[state_name],
                "abbr": state_name,
                "Biden": groups.get("Biden", 0),
                "Trump": groups.get("Trump", 0),
            }
        )
    pd.DataFrame(rst).to_csv("data/csv/0501-states.csv")


def rescale_opinion(input_users, state_name):
    dict_names = us.states.mapping('abbr', 'name')
    if state_name == "US":
        cen = pd.read_csv(f"data/census/US.csv").set_index("category")
    else:
        cen = pd.read_csv(f"data/census/{dict_names[state_name]}.csv").set_index("category")
    w = cen.percent.to_list()
    
    camps = [
         "Bernie Sanders", 
         "Joe Biden",
    ]
    # print(input_users)
    
    if state_name == "US":
        users_tmp = input_users
    else:
        users_tmp = input_users[input_users.state==state_name]
        
    groups = users_tmp.groupby(["age_range", "gender", "Camp"]).size()
    # print(groups)
    _rst = {}
    _analysis={}

    # cross-classification weight table
    # first index: female -> 2, male -> 1
    # second index: ages from young to old
    G12 = sum(groups[">=18, <30"]["Female"])
    G11 = sum(groups[">=18, <30"]["Male"])
    G22 = sum(groups[">=30, <50"]["Female"])
    G21 = sum(groups[">=30, <50"]["Male"])
    G32 = sum(groups[">=50, <65"]["Female"])
    G31 = sum(groups[">=50, <65"]["Male"])
    G42 = sum(groups[">=65"]["Female"])
    G41 = sum(groups[">=65"]["Male"])

    for _camp in camps:
        T12 = groups[">=18, <30"]["Female"].get(_camp, 0)
        T11 = groups[">=18, <30"]["Male"].get(_camp, 0)
        T22 = groups[">=30, <50"]["Female"].get(_camp, 0)
        T21 = groups[">=30, <50"]["Male"].get(_camp, 0)
        T32 = groups[">=50, <65"]["Female"].get(_camp, 0)
        T31 = groups[">=50, <65"]["Male"].get(_camp, 0)
        T42 = groups[">=65"]["Female"].get(_camp, 0)
        T41 = groups[">=65"]["Male"].get(_camp, 0)

        r = T12 / G12 * w[0] + T11 / G11 * w[1] \
            + T22 / G22 * w[0] + T21 / G21 * w[1] \
            + T32 / G32 * w[0] + T31 / G31 * w[1] \
            + T42 / G42 * w[0] + T41 / G41 * w[1] \

        _rst[_camp] = round(r * 100, 1)
        _analysis[_camp] = [T12, T11, T22, T21, T32, T31, T42, T41]
        
    return _rst, _analysis

def rescale_per_state():
    users = load_users_union()

    rst = []

    for state_name in USA_STATES:
        users_tmp = users[users.state == state_name]
        groups = users_tmp.groupby(["Camp"]).size()
        print(state_name, groups)
        rst.append(
            {
                "state": USA_ADDR_NAME[state_name],
                "abbr": state_name,
                "Biden": groups.get("Biden", 0),
                "Trump": groups.get("Trump", 0),
            }
        )
    pd.DataFrame(rst).to_csv("data/csv/0501-states.csv")

            input_users = pd.read_csv(f"disk/users-culFrom09_DEM/{dt_str}.csv").set_index("uid")
            
            for k, v in dict_names.items():
                if k in SuperTuesday:
                    # print(k)
                    rst, analysis = rescale_opinion(input_users, k)
                    analysis["state"]=k
                    analysis["dt"] = dt_str
                    rst["state"] = k
                    rst["dt"] = dt_str
                    #rst["dt"] = i+2
                    #print(rst)
                    rsts.append(rst)
                    Analysis.append(analysis)
                    
    #Analysis=pd.DataFrame(Analysis).set_index("dt")
    #Analysis=Analysis.sort_index()
    rsts = pd.DataFrame(rsts).set_index("dt")
    rsts = rsts.sort_index()
    rsts.to_csv("data/csv/Democratic_new.csv")
    #Analysis.to_csv("data/csv/Deep_study.csv")


if __name__ == "__main__":
    pred_per_state()


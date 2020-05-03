# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_reweight.py                             :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/05/03 09:01:29 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/03 22:42:00 by Kay Zhou         ###   ########.fr        #
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


def load_users_location(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    print(users)
    return users


def load_users_opinion(in_name):
    users = pd.read_csv(in_name).set_index("uid")
    return users


def load_users_union():
    users = load_users_opinion("disk/users-culFrom01/2020-04-30.csv")
    u2 = load_users_location("disk/users-location/2020-04-30.csv")
    users = users.join(u2, how="inner")
    users["Camp"] = "None"
    users[users["0"] >= users["1"]]["Camp"] = "Biden"
    users[users["0"] < users["1"]]["Camp"] = "Trump"
    print(users)
    return users


def pred_per_state():
    users = load_users_union()

    for state_name in USA_STATES:
        users_tmp = users[users.state == state_name]
        groups = users_tmp.groupby(["Camp"]).size()
        print(state_name, groups)


if __name__ == "__main__":
    pred_per_state()

# def rescale_opinion(input_users, state_name):
#     if state_name == "US":
#         cen = pd.read_csv(f"data/census/US.csv").set_index("category")
#     else:
#         cen = pd.read_csv(f"data/census/{dict_names[state_name]}.csv").set_index("category")
#     # print("After reweighting!")
#     w = cen.percent.to_list()
    
#     camps = [
#          "Bernie Sanders", 
#          "Joe Biden",
#          #"Elizabeth Warren", 
#          #"Mike Bloomberg",
#          #"Pete Buttigieg",
#          #"Others",
#     ]
#     # print(input_users)
    
#     if state_name == "US":
#         users_tmp = input_users
#     else:
#         #print('here')
#         users_tmp = input_users[input_users.state==state_name]
        
#     groups = users_tmp.groupby(["age_range", "gender", "Camp"]).size()
#     # print(groups)
#     _rst = {}
#     _analysis={}

#     for _camp in camps:
#         g1= groups[">=18, <30"]["Female"].get(_camp, 0)
#         g2=groups[">=18, <30"]["Male"].get(_camp, 0)
#         g3=groups[">=30, <50"]["Female"].get(_camp, 0)
#         g4=groups[">=30, <50"]["Male"].get(_camp, 0)
#         g5=groups[">=50, <65"]["Female"].get(_camp, 0)
#         g6=groups[">=50, <65"]["Male"].get(_camp, 0)
#         g7= groups[">=65"]["Female"].get(_camp, 0)
#         g8=groups[">=65"]["Male"].get(_camp, 0)
#         r =g1 / sum(groups[">=18, <30"]["Female"]) * w[0] \
#         + g2 / sum(groups[">=18, <30"]["Male"]) * w[1] \
#         + g3 / sum(groups[">=30, <50"]["Female"]) * w[2] \
#         + g4 / sum(groups[">=30, <50"]["Male"]) * w[3] \
#         + g5 / sum(groups[">=50, <65"]["Female"]) * w[4] \
#         + g6 / sum(groups[">=50, <65"]["Male"]) * w[5] \
#         + g7 / sum(groups[">=65"]["Female"]) * w[6] \
#         + g8 / sum(groups[">=65"]["Male"]) * w[7]
#         # print(_camp, round(r, 3))
#         _rst[_camp] = round(r * 100, 1)
#         _analysis[_camp]=[g1,g2,g3,g4,g5,g6,g7,g8]
        
#     return _rst, _analysis

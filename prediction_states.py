# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_states.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun Zhou <zhenkun91@outlook.com>       +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2021/07/29 09:23:27 by Zhenkun Zho       #+#    #+#              #
#    Updated: 2021/07/29 09:23:27 by Zhenkun Zho      ###   ########.fr        #
#                                                                              #
# **************************************************************************** #


from SQLite_handler import *
import rapidjson as json
from tqdm import tqdm
import os

# 0 > JB; 1 > DT
US_states = ['NY', 'DC', 'IN', 'AR', 'WY', 'ME', 'TX', 'NH', 'CO', 'CA', 'IL',
             'WA', 'VA', 'FL', 'MA', 'OR', 'AZ', 'MT', 'MN', 'NE', 'TN', 'OH',
             'NJ', 'NV', 'KY', 'UT', 'NC', 'SC', 'PA', 'NM', 'KS', 'GA', 'MI',
             'WI', 'AK', 'MS', 'MD', 'LA', 'HI', 'MO', 'AL', 'CT', 'OK', 'IA',
             'WV', 'RI', 'SD', 'VT', 'ND', 'ID', 'DE']


def get_state_results(json_file_name):
    # 读入用户的州信息
    user_info = {}
    for line in tqdm(open("D:/US2020/user-state.txt", encoding='utf8')):
        w = line.strip().split("\t")
        user_info[w[0]] = w[:-1] # 州结果

    user_result = json.load(open(json_file_name, encoding='utf8'))
    u_table = []
    for u, v in user_result.items():
        if u not in user_info:
            continue
        u_state = user_info[u]
        if v[0] > v[1]:
            opinion = "B"
        elif v[0] < v[1]:
            opinion = "T"
        elif v[0] == v[1]:
            opinion = "U"

        u_table.append({
            "uid": u,
            "state": u_state,
            "opinion": opinion
        })
    u_df = pd.DataFrame(u_table).set_index('uid')
    u_result = u_df.groupby("state")["opinion"].value_counts()
    u_result.to_csv(f"data/states_{json_file_name}.csv")


if __name__ == "__main__":
    # v1 > only Trump and Biden
    # v2 > Republicans and Democrats
    get_state_results('data/users-14days-v1-0.5/2020-10-26.json')

    # get_state_results('data/users-cumFrom01-v1-0.5/2020-10-31.json')
    # get_state_results('data/users-cumFrom01-v2-0.5/2020-10-31.json')
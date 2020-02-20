# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    undecided.py                                       :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/07/26 16:45:31 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/08/09 21:09:10 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *
from SQLite_handler import get_session, get_ori_users_v2


def calculate_undecided_users(dt, win=7):
    """
    利用同质性判断undecided users的opinion
    """
    dt_str = dt.format("YYYYMMDD")
    # users = json.load(open(f"disk/users-14days/{dt_str}.txt"))
    users = json.load(open(f"/home/alex/kayzhou/Argentina_election/disk/data/2019-08-08/supporters_.75_14-{dt_str}.json"))

    K_users = set()
    M_users = set()

    undecided_users = dict()
    for u, v in users.items():
        u = int(u)
        if v["I"] > 0:
            continue
        if v["K"] == v["M"]:
            undecided_users[u] = {
                "K": 0,
                "M": 0,
            }
        elif v["K"] > v["M"]:
            K_users.add(u)
        elif v["M"] > v["K"]:
            M_users.add(u)

    K_cnt = 0
    M_cnt = 0
    U_cnt = 0
    U1_cnt = 0

    uK_users = []
    uM_users = []

    friends_of_users = get_ori_users_v2(dt.add(days=-14), dt, set(undecided_users.keys()))

    for u in tqdm(undecided_users):
        for friend in friends_of_users[u]:
            # print(u, friend)
            if friend in K_users:
                undecided_users[u]["K"] += 1
            elif friend in M_users:
                undecided_users[u]["M"] += 1
        
        if undecided_users[u]["K"] > undecided_users[u]["M"]:
            K_cnt += 1
            uK_users.append(u)
        elif undecided_users[u]["K"] < undecided_users[u]["M"]:
            M_cnt += 1
            uM_users.append(u)
        else:
            U_cnt += 1
            if undecided_users[u]["K"] >= 1:
                U1_cnt += 1

    print(dt_str, K_cnt, M_cnt, U_cnt, U1_cnt)

    json.dump(uK_users, open("disk/data/uK_users.json", "w"))
    json.dump(uM_users, open("disk/data/uM_users.json", "w"))

    
    # 7 means always 7% users are undecided
    return {
            "dt": dt_str,
            "K":K_cnt * 7 / len(undecided_users),
            "M": M_cnt * 7 / len(undecided_users),
            "U": U_cnt * 7 / len(undecided_users)
        }


rst = []
start = pendulum.datetime(2019, 8, 9, tz="UTC")
end = pendulum.datetime(2019, 8, 9, tz="UTC")
_period = pendulum.Period(start, end)
for dt in _period:
    rst.append(calculate_undecided_users(dt))

# pd.DataFrame(rst).set_index("dt").to_csv("web/data/undecided_users-7days-0720-0808.csv")

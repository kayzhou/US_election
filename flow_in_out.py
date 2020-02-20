# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    flow_in_out.py                                     :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/07/15 11:18:59 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/07/26 16:33:31 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from my_weapon import *

start = pendulum.datetime(2019, 7, 14, tz="UTC")
end = pendulum.datetime(2019, 7, 26, tz="UTC")
_period = pendulum.Period(start, end)

def get_opnion(user_v):
    if user_v["K"] > user_v["M"]:
        return "K"
    elif user_v["M"] > user_v["K"]:
        return "M"
    elif user_v["M"] == user_v["K"] and user_v["M"] > 0:
        return "U"

last_user_sets = None


per_rsts = []
tol_per_rsts = []
rsts = []

for dt in _period:
    if dt.day_of_week != 1:
        continue
    print(dt, dt.day_of_week)
    user_sets = defaultdict(set)
    users = json.load(open(f"disk/users/{dt.to_date_string()}.txt"))
    for uid, u_v in users.items():
        user_sets[get_opnion(u_v)].add(uid)

    if last_user_sets is None:
        last_user_sets = user_sets
        last_num = len(user_sets["M"]) + len(user_sets["K"]) + len(user_sets["U"])
        continue

    new_K = user_sets["K"] - last_user_sets["K"]
    new_K_from_M = [u for u in new_K if u in last_user_sets["M"]]
    new_K_from_U = [u for u in new_K if u in last_user_sets["U"]]

    new_M = user_sets["M"] - last_user_sets["M"]
    new_M_from_K = [u for u in new_M if u in last_user_sets["K"]]
    new_M_from_U = [u for u in new_M if u in last_user_sets["U"]]

    new_U = user_sets["U"] - last_user_sets["U"]
    new_U_from_K = [u for u in new_U if u in last_user_sets["K"]]
    new_U_from_M = [u for u in new_U if u in last_user_sets["M"]]

    per_rsts.append({
        "dt": dt.to_date_string(),
        # "M - K": len(new_K_from_M) / len(last_user_sets["M"]),
        "U - K": len(new_K_from_U) / len(last_user_sets["U"]) * 100,
        # "K - M": len(new_M_from_K) / len(last_user_sets["K"]),
        "U - M": len(new_M_from_U) / len(last_user_sets["U"]) * 100,
        "K - U": len(new_U_from_K) / len(last_user_sets["K"]) * 100,
        "M - U": len(new_U_from_M) / len(last_user_sets["M"]) * 100,
    })

    tol_per_rsts.append({
        "dt": dt.to_date_string(),
        # "M - K": len(new_K_from_M) / last_num,
        "U - K": len(new_K_from_U) / last_num * 100,
        # "K - M": len(new_M_from_K) / last_num,
        "U - M": len(new_M_from_U) / last_num * 100,
        "K - U": len(new_U_from_K) / last_num * 100,
        "M - U": len(new_U_from_M) / last_num * 100,
    })
    
    rsts.append({
        "dt": dt.to_date_string(),
        # "M - K": len(new_K_from_M),
        "U - K": len(new_K_from_U),
        # "K - M": len(new_M_from_K),
        "U - M": len(new_M_from_U),
        "K - U": len(new_U_from_K),
        "M - U": len(new_U_from_M),
    })

    last_user_sets = user_sets

pd.DataFrame(rsts).set_index("dt").to_csv("web/data/flow-in-out.csv")
pd.DataFrame(per_rsts).set_index("dt").to_csv("web/data/flow-in-out-percent.csv", float_format='%2.1f')
pd.DataFrame(tol_per_rsts).set_index("dt").to_csv("web/data/flow-in-out-tol-percent.csv", float_format='%2.1f')

# pd.DataFrame(rsts).set_index("dt").to_csv("web/data/flow-in-out-30.csv")
# pd.DataFrame(per_rsts).set_index("dt").to_csv("web/data/flow-in-out-percent-30.csv", float_format='%2.1f')
# pd.DataFrame(tol_per_rsts).set_index("dt").to_csv("web/data/flow-in-out-tol-percent-30.csv", float_format='%2.1f')


   

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_from_db.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/19 04:01:00 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/26 08:04:02 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *

# 0 > JB; 1 > DT
US_states = ['NY', 'DC', 'IN', 'AR', 'WY', 'ME', 'TX', 'NH', 'CO', 'CA', 'IL',
             'WA', 'VA', 'FL', 'MA', 'OR', 'AZ', 'MT', 'MN', 'NE', 'TN', 'OH',
             'NJ', 'NV', 'KY', 'UT', 'NC', 'SC', 'PA', 'NM', 'KS', 'GA', 'MI',
             'WI', 'AK', 'MS', 'MD', 'LA', 'HI', 'MO', 'AL', 'CT', 'OK', 'IA',
             'WV', 'RI', 'SD', 'VT', 'ND', 'ID', 'DE']
# USERS = pd.read_csv("disk/users-face/2020-04-02.csv").set_index("uid")
# USERS_STATE = pd.read_csv("disk/users-location/2020-04-02.csv",
#                           usecols=["uid","state"],
#                           error_bad_lines=False).set_index("uid")
# USERS_STSTE_GENDER_AGE = USERS.join(USERS_STATE, how="inner")
# SET_USERS = set(USERS_STSTE_GENDER_AGE.index)
# SET_USERS = set(USERS_STATE.index)


def save_user_snapshot(sess, now):
    users = {}
    for t in get_tweets(sess, now, now.add(days=1)):
        uid = t.user_id
        if uid not in users:
            users[uid] = [0, 0] # Biden and Trump
        users[uid][t.camp] += 1
    print("# of users:", len(users))
    with open(f"data/users-day/{now.to_date_string()}.csv", "w") as f:
        f.write("uid,0,1\n")
        for u, v in users.items():
            f.write(f"{u},{v[0]},{v[1]}\n")


def save_user_csv(sess, start, end):
    for dt in pendulum.period(start, end.add(days=-1)):
        print(dt)
        save_user_snapshot(sess, dt)


# def read_users_from_csv(in_name):
#     print("Reading users from csv ...", in_name)
#     users = pd.read_csv(in_name).set_index("uid").T.to_dict()
#     _users = {}
#     for u, v in users.items():
#         _users[u] = np.array([v["0"], v["1"]])
#     print("# of users:", len(_users))
#     return _users
def read_users_from_csv(in_name):
    print("Loading users from:", in_name)
    users = {}
    for line in tqdm(open(in_name)):
        if line.startswith("uid"):
            continue
        uid, v0, v1 = line.strip().split(",")
        users[uid] = [int(v0), int(v1)] # 0 for Biden, 1 for Trump
    print("# of users:", len(users))
    return users


# def read_users_from_csv_from_uids(in_name, set_uids):
#     print("Reading users from csv ...", in_name)
#     users = pd.read_csv(in_name).set_index("uid").T.to_dict()
#     _users = {}
#     for u, v in users.items():
#         if u in set_uids:
#             _users[u] = np.array([v["0"], v["1"]])
#     print("# of users:", len(_users))
#     return _users
def read_users_from_csv_from_uids(in_name, set_uids):
    print("Loading users from:", in_name)
    users = {}
    for line in open(in_name):
        if line.startswith("uid"):
            continue
        uid, v0, v1 = line.strip().split(",")
        if uid in set_uids:
            users[uid] = [int(v0), int(v1)]
    print("# of users:", len(users))
    return users


def union_users_from_dict(users_groups):
    all_users = {}
    for users in users_groups:
        for u, v in users.items():
            if u not in all_users:
                all_users[u] = v
            else:
                all_users[u][0] += v[0]
                all_users[u][1] += v[1]
    return all_users


def union_users_from_yesterday_and_today(yes_users, today_users):
    all_users = yes_users
    for u, v in today_users.items():
        if u not in all_users:
            all_users[u] = [0, 0]
        all_users[u][0] += v[0]
        all_users[u][1] += v[1]
    return all_users


def write_union_users_csv(union_users_dict, out_dir, dt):
    print("Saving ...", f"disk/{out_dir}/{dt}.csv")
    with open(f"disk/{out_dir}/{dt}.csv", "w") as f:
        f.write("uid,0,1\n")
        for u, v in union_users_dict.items():
            f.write(f"{u},{v[0]},{v[1]}\n")


def write_union_users_csv_v2(union_users_dict, out_dir, dt):
    # 需要结合已有数据，减少数据保存量
    num2label = {
        0: "JB",
        1: "DT",
    }
    print("saving ...", f"disk/{out_dir}/{dt}.csv")
    rsts = []
    if union_users_dict:
        for u, v in union_users_dict.items():
            if u in SET_USERS:
                max_i = v.argmax()
                rsts.append({"uid": u, "Camp": num2label[max_i]})

        _users = pd.DataFrame(rsts).set_index("uid").join(
            USERS_STSTE_GENDER_AGE, how="inner")
        _users.to_csv(f"disk/{out_dir}/{dt}.csv")


def get_share_from_users_dict(users_dict):
    counts = {
        0: 0, # Biden
        1: 0, # Trump
        2: 0, # Undecided
    }
    for _, v in users_dict.items():
        if v[0] > v[1]:
            counts[0] += 1
        elif v[0] < v[1]:
            counts[1] += 1
        else:
            counts[2] += 1
    return counts


def get_share_from_csv(csv_name):
    users_dict = read_users_from_csv(csv_name)
    return get_share_from_users_dict(users_dict)


def calculate_window_share(start, end, win=14, save_csv=None):
    rsts = []
    users_cache = {}

    for dt in pendulum.period(start, end):

        if dt == start:
            continue

        # print(dt)
        win_dts = pendulum.period(dt.add(days=-win), dt.add(days=-1))

        users_groups = []
        for win_dt in win_dts:
            win_dt_str = win_dt.to_date_string()
            if win_dt_str in users_cache:
                _u = users_cache[win_dt_str]
            else:
                _u = read_users_from_csv(f"data/users-day/{win_dt_str}.csv")
                users_cache[win_dt_str] = _u
            users_groups.append(_u)

        users_cache.pop(dt.add(days=-win).to_date_string())

        union_users_dict = union_users_from_dict(users_groups)
        write_union_users_csv(
            union_users_dict, f"users-{win}days", dt.to_date_string())

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    if save_csv:
        rsts = pd.DataFrame(rsts).set_index("dt")
        rsts.to_csv(
            f"data/csv/results-{win}days-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")


def calculate_cumulative_share(start, end, super_start_month="01", save_users=True, save_db=True):
    # from super_start (include) to -1
    if super_start_month == "09":
        super_start = pendulum.datetime(2019, 9, 1)
    elif super_start_month == "11":
        super_start = pendulum.datetime(2019, 11, 1)
    elif super_start_month == "01":
        super_start = pendulum.datetime(2020, 1, 1)
    elif super_start_month == "03":
        super_start = pendulum.datetime(2020, 3, 1)
    elif super_start_month == "05":
        super_start = pendulum.datetime(2020, 3, 1)
    else:
        super_start = "Unknown super_start_month"

    rsts = []
    # super_start = start
    yesterday_users = None

    for dt in pendulum.period(start, end):

        if dt <= super_start:
            print("Error: start <= super_start.")
            return -1

        elif dt == super_start.add(days=1):
            union_users_dict = read_users_from_csv(f"data/users-day/{super_start.to_date_string()}.csv")
            print("载入super_start数据~", super_start.to_date_string())
            write_union_users_csv(union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())

        else:
            # just from the cumulative yesterday
            # So I must have the yesterday's cumulative csv
            if yesterday_users is None:
                print("Loading yesterday users' csv at （载入初始数据）", dt.add(days=-1))
                yesterday_users = read_users_from_csv(
                    f"disk/users-culFrom{super_start_month}/{dt.add(days=-1).to_date_string()}.csv")

            today_users = read_users_from_csv(f"data/users-day/{dt.add(days=-1).to_date_string()}.csv")
            union_users_dict = union_users_from_yesterday_and_today(yesterday_users, today_users)
            yesterday_users = union_users_dict # Today will be the yesterday.
            if save_users:
                write_union_users_csv(union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    pd_rsts = pd.DataFrame(rsts).set_index("dt")
    pd_rsts.index = pd.to_datetime(pd_rsts.index)
    pd_rsts = pd_rsts.rename(columns={0: "Joe Biden", 1: "Donald Trump", 2: "Undecided"})
    pd_rsts.to_csv(f"data/csv/results-cumFrom{super_start_month}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")

    if save_db:
        rsts = [{
                "_id": r["dt"] + "-USA",
                "dt": r["dt"], 
                "state": "USA",
                "Biden": r[0],
                "Trump": r[1],
                "Undec": r[2],
            } for r in rsts]
        cumulative_prediction_results_to_db(rsts)


def calculate_t0_share(start, super_end, save_csv=None):
    rsts = []

    # super_start = start
    next_users = None

    for i, dt in enumerate(pendulum.period(super_end, start)):
        print(i, dt)

        if dt == super_end:
            union_users_dict = read_users_from_csv(
                f"data/users-day_after_BT_m2_all/{super_end.add(days=-1).to_date_string()}.csv")
            write_union_users_csv(
                union_users_dict, f"users-To{super_end.to_date_string()}", "1")

        else:
            if next_users is None:
                print("Loading next users' csv.")
                next_users = read_users_from_csv(
                    f"disk/users-To{super_end.to_date_string()}/1.csv")

            today_users = read_users_from_csv(
                f"data/users-day_after_BT_m2_all/{dt.add(days=-1).to_date_string()}.csv")
            union_users_dict = union_users_from_yesterday_and_today(
                today_users, next_users)
            next_users = union_users_dict
            write_union_users_csv_v2(
                union_users_dict, f"users-To{super_end.to_date_string()}", str(i+1))

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("dt")

    if save_csv:
        rsts.to_csv(f"data/csv/results-To{super_end.to_date_string()}.csv")

    return rsts


def load_df_user_loc(dt):
    print("Loading df_user_loc ...")
    df_users = pd.read_csv(f"disk/users-location/{dt.to_date_string()}.csv",
                           usecols=["uid", "state"]).set_index("uid")
    return df_users


def predict_from_location_from_csv(csv_file, save_csv=None):
    df_user = load_df_user_loc("?")
    users_dict = read_users_from_csv(csv_file)
    rsts = []
    for _s in US_states:
        uid_in_s = set(df_user[df_user.state == _s].index)
        users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
        print(_s, len(uid_in_s), len(users_dict))
        # write_union_users_csv(users_dict, out_dir, _s + ".csv")
        rst = get_share_from_users_dict(users_in_s)
        rst["state"] = _s
        print(rst)
        rsts.append(rst)
    rsts = pd.DataFrame(rsts).set_index("state")

    if save_csv:
        rsts.to_csv(save_csv)


def predict_from_location(start, end, out_dir, save_csv=True, save_users=False):
    # df_user = load_df_user_loc(end)
    # 需要已经保存了每天预测的用户列表
    df_user = pd.read_csv(f"disk/users-location/2020-07-01.csv", usecols=["uid", "state"]).set_index("uid")

    rsts = []
    for dt in pendulum.period(start, end):
        if dt == start:
            continue

        csv_file = f"disk/users-{out_dir}/{dt.to_date_string()}.csv"
        users_dict = read_users_from_csv(csv_file)

        for _s in US_states:
            # 选择每个洲的结果
            uid_in_s = set(df_user[df_user.state == _s].index)
            users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
            print(_s, len(uid_in_s), len(users_dict))

            if save_users:
                write_union_users_csv(users_dict, out_dir + "_loc", dt.to_date_string() + "-" + _s)

            rst = get_share_from_users_dict(users_in_s)
            rst["id"] = _s + ":" + dt.to_date_string()
            rst["dt"] = dt.to_date_string()
            rst["state"] = _s
            print(rst)
            rsts.append(rst)

    if save_csv:
        rsts = pd.DataFrame(rsts).set_index("id")
        rsts.to_csv(f"data/csv/results-states-{out_dir}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    return rsts


def predict_t0_from_location(in_dir, state):
    df_user = load_df_user_loc()
    uid_in_s = set(df_user[df_user.state == state].index)

    rsts = []
    for i in range(1, 31):
        csv_file = f"disk/users-To{in_dir}/{i}.csv"
        users_dict = read_users_from_csv(csv_file)
        users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
        print(state, len(uid_in_s), len(users_dict))
        rst = get_share_from_users_dict(users_in_s)
        rst["days"] = i
        rst["state"] = state
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("days")
    rsts.to_csv(f"data/csv/results-t0-{state}.csv")


def predict_from_location_superStart(start, end, state):
    """
    each state have the super_start
    """
    import pathlib

    df_user = load_df_user_loc(end)
    uid_in_s = set(df_user[df_user.state == state].index)

    superStart_of_states = {
        "IA": pendulum.datetime(2020, 2, 1, tz="UTC"),  # IA
        "NH": pendulum.datetime(2020, 2, 3, tz="UTC"),  # NH
        # "NV": pendulum.datetime(2020, 1, 22, tz="UTC"), # NV
    }

    super_start = superStart_of_states[state]
    # super_start = start
    yesterday_users = None

    pathlib.Path(
        f"disk/users-From{super_start.to_date_string()}-{state}").mkdir(exist_ok=True)

    rsts = []
    for dt in pendulum.period(start, end):

        if dt <= super_start:  # start in the function is always the next day of super_start
            print("Error: start <= super_start of cumulative prediction.")
            return -1

        elif dt == super_start.add(days=1):
            union_users_dict = read_users_from_csv_from_uids(
                f"data/users-day/{super_start.to_date_string()}.csv", uid_in_s)
            write_union_users_csv(
                union_users_dict, f"users-From{super_start.to_date_string()}-{state}", dt.to_date_string())

        else:
            # just from the cumulative yesterday
            # So I must have the yesterday's cumulative csv
            if yesterday_users is None:
                print("Loading yesterday users' csv at", dt.add(days=-1))
                yesterday_users = read_users_from_csv_from_uids(
                    f"disk/users-From{super_start.to_date_string()}-{state}/{dt.add(days=-1).to_date_string()}.csv",
                    uid_in_s)
            today_users = read_users_from_csv_from_uids(
                f"data/users-day/{dt.add(days=-1).to_date_string()}.csv", uid_in_s)
            union_users_dict = union_users_from_yesterday_and_today(
                yesterday_users, today_users)
            yesterday_users = union_users_dict

        if dt == end:
            print("Writing cumulative users' csv at", dt)
            write_union_users_csv(
                union_users_dict, f"users-From{super_start.to_date_string()}-{state}", dt.to_date_string())

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        rst["state"] = state
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("dt")
    rsts.to_csv(
        f"data/csv/results-From{super_start.to_date_string()}-{state}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")


def daily_prediction():
    end = pendulum.today(tz="UTC")  # not include this date
    start = pendulum.yesterday(tz="UTC")  # include this date

    # nation From01
    calculate_cumulative_share(start, end, super_start_month="01")

    # states 40days
    calculate_window_share(start, end, win=40)
    predict_from_location(start, end, out_dir="40days")


if __name__ == "__main__":
    # start = pendulum.datetime(2020, 1, 1, tz="UTC")
    # end = pendulum.datetime(2020, 7, 10, tz="UTC")
    # sess = get_session()
    # -- to database --
    # tweets_to_db(sess, start, end, clear=False)             
    # -- save users' snapshot --
    # save_user_csv(sess, start, end)
    # sess.close()

    # run it per day
    # daily_prediction()

    # -- window start --
    # 7 days
    # start = pendulum.datetime(2020, 1, 8, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=7, save_csv=True)

    # 14 days
    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 6, 21, tz="UTC")
    # calculate_window_share(start, end, win=14, save_csv=True)
    # -- window end --

    # -- cumulative start --
    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 7, 10, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="01")

    # start = pendulum.datetime(2020, 3, 2, tz="UTC")
    # end = pendulum.datetime(2020, 7, 10, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="03", save_db=False)
    # -- cumulative end --

    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="14days")

    start = pendulum.datetime(2020, 1, 2, tz="UTC")
    end = pendulum.datetime(2020, 7, 1, tz="UTC")
    predict_from_location(start, end, out_dir="culFrom01")

    # t0
    # start = pendulum.datetime(2019, 9, 4, tz="UTC")
    # end = pendulum.datetime(2020, 3, 10, tz="UTC")
    # calculate_t0_share(start, end, save_csv=True)

    # start = pendulum.datetime(2020, 1, 11, tz="UTC")
    # end = pendulum.datetime(2020, 2, 11, tz="UTC")
    # calculate_t0_share(start, end, save_csv=True)

    # start = pendulum.datetime(2020, 1, 3, tz="UTC")
    # end = pendulum.datetime(2020, 2, 3, tz="UTC")
    # calculate_t0_share(start, end, save_csv=True)

    # t0 state
    # predict_t0_from_location("2020-02-03", "IA")
    # predict_t0_from_location("2020-02-11", "NH")
    # predict_t0_from_location("2020-02-22", "NV")

    # different initial dates for states
    # end = pendulum.datetime(2020, 3, 2, tz="UTC")

    # start = pendulum.datetime(2020, 2, 2, tz="UTC")
    # predict_from_location_superStart(start, end, "IA")

    # start = pendulum.datetime(2020, 2, 4, tz="UTC")
    # predict_from_location_superStart(start, end, "NH")

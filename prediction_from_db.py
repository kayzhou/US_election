# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_from_db.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/19 04:01:00 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/10/25 15:07:46 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *
import os

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


def save_bots(out_name):
    set_users = set()
    with open(out_name, "w") as f:
        months = ["10", "09", "08", "07", "06"]
        for m in months:
            print(m)
            for line in open(f"data/2020{m}-tweets-prediction.txt"):
                d = line.strip().split(",")
                if d[3] != "None" and d[1] not in set_users:
                    f.write(d[1] + "\n")
                    set_users.add(d[1])
                    

def load_bots(in_name):
    all_bots = set()
    for line in open(in_name):
        all_bots.add(line.strip())
    print("# of bots:", len(all_bots))
    return all_bots
# ALL_BOTS = load_bots("data/users-profile/20201010bots.txt")
    

def save_user_snapshot_json(in_names, p=0.5):
    dict_date_users = {}
    # global ALL_BOTS
    set_bots = set()
    for in_name in in_names:
        print("save_user_snapshot_json", in_name)
        for line in tqdm(open(in_name)):
            d = line.strip().split(",")
            # 先不查重tweet_id
            uid = d[0]
            # if uid in ALL_BOTS or d[3] != "None":
            #     continue
            date = d[1]
            source = d[2]
            if source != "None" or uid in set_bots:
                set_bots.add(uid)
                continue
            proba = float(d[3])
            query = d[4].lower()
            if not ("trump" in query or "biden" in query or "~" in query):
                continue

            if date not in dict_date_users:
                dict_date_users[date] = {}
            if uid not in dict_date_users[date]:
                dict_date_users[date][uid] = [0, 0]

            # 0 for Biden, 1 for Trump
            if proba <= (1 - p):
                dict_date_users[date][uid][0] += 1
            elif proba > p:
                dict_date_users[date][uid][1] += 1

    for date, dict_uid in dict_date_users.items():
        if p == 0.5:
            f_name = f"data/users-day-onlyTB/{date}.json"
        else:
            f_name = f"data/users-day-onlyTB-{p}/{date}.json"
        if os.path.exists(f_name):
            print(f_name, "已经存在。")
        else:
            json.dump(dict_uid, open(f_name, "w"))


def save_user_snapshot(sess, now):
    users = {}
    # for t in get_tweets(sess, now, now.add(days=1)):
    for t in get_tweets_proba(sess, now, now.add(days=1)):
        uid = t.user_id
        if uid not in users:
            users[uid] = [0, 0] # Biden and Trump
        users[uid][t.camp] += 1
    print("# of users:", len(users))
    with open(f"data/users-day-0.66/{now.to_date_string()}.csv", "w") as f:
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
        if uid in all_bots:
            continue
        users[uid] = [int(v0), int(v1)] # 0 for Biden, 1 for Trump
    print("# of users:", len(users))
    return users


def read_users_from_json(in_name):
    print("Loading users from:", in_name)
    if os.path.exists(in_name):
        users = json.load(open(in_name))
    else:
        print("Not exist")
        users = {}
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


def write_union_users_json(union_users_dict, out_dir, dt):
    print("Saving ...", f"data/{out_dir}/{dt}.csv")
    with open(f"data/{out_dir}/{dt}.json", "w") as f:
        json.dump(union_users_dict, f)


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
        0: 0,  # Biden
        1: 0,  # Trump
        2: 0,  # Undecided
    }
    for _, v in users_dict.items():
        if v[0] > v[1]:
            counts[0] += 1
        elif v[0] < v[1]:
            counts[1] += 1
        elif v[0] == v[1] and v[0] > 0:
            counts[2] += 1
    return counts


def get_share_from_csv(csv_name):
    users_dict = read_users_from_csv(csv_name)
    return get_share_from_users_dict(users_dict)


def calculate_window_share_size_1(start, end, save_csv=True):
    rsts = []
    for dt in pendulum.period(start, end):
        _u = read_users_from_csv(f"data/users-day/{dt.to_date_string()}.csv")
        rst = get_share_from_users_dict(_u)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    if save_csv:
        rsts = pd.DataFrame(rsts).set_index("dt")
        rsts.to_csv(f"data/csv/results-1day-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")


def calculate_window_share(start, end, win=14, save_users=True):
    """VIP 1

    Args:
        start ([type]): [description]
        end ([type]): [description]
        win (int, optional): [description]. Defaults to 14.
        save_csv (bool, optional): [description]. Defaults to True.
    """
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
                _u = read_users_from_json(f"data/users-day/{win_dt_str}.json")
                # _u = read_users_from_json(f"data/users-day-0.66/{win_dt_str}.json")
                users_cache[win_dt_str] = _u
            users_groups.append(_u)

        users_cache.pop(dt.add(days=-win).to_date_string())

        union_users_dict = union_users_from_dict(users_groups)
        if save_users and dt.day_of_week == 1:
            write_union_users_json(union_users_dict, f"users-{win}days", dt.to_date_string())
            # write_union_users_json(union_users_dict, f"users-{win}days-0.66", dt.to_date_string())

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("dt")
    rsts = rsts.rename(columns={0: "Biden", 1: "Trump", 2: "Undecided"})
    rsts.to_csv(
        f"data/csv/{win}days-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")


def calculate_cumulative_share(start, end, super_start_month="01", save_users=True):
    """VIP 2

    Args:
        start ([type]): [description]
        end ([type]): [description]
        super_start_month (str, optional): [description]. Defaults to "01".
        save_users (bool, optional): [description]. Defaults to True.
        save_db (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """
    # from super_start (include) to -1
    if super_start_month == "01":
        super_start = pendulum.datetime(2020, 1, 1)
    elif super_start_month == "06":
        super_start = pendulum.datetime(2020, 6, 1)
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
            union_users_dict = read_users_from_json(f"data/users-day/{super_start.to_date_string()}.json")
            # union_users_dict = read_users_from_json(f"data/users-day-0.66/{super_start.to_date_string()}.json")
            print("Loading data on super_start ...", super_start.to_date_string())
            write_union_users_json(union_users_dict, f"users-cumFrom{super_start_month}", dt.to_date_string())

        else:
            # just from the cumulative yesterday
            # So I must have the yesterday's cumulative csv
            if yesterday_users is None:
                print("Loading yesterday users' json at （载入初始数据）", dt.add(days=-1))
                yesterday_users = read_users_from_json(
                    f"disk/users-cumFrom{super_start_month}/{dt.add(days=-1).to_date_string()}.json")
                    # f"disk/users-cumFrom{super_start_month}-0.66/{dt.add(days=-1).to_date_string()}.json")

            today_users = read_users_from_json(f"data/users-day/{dt.add(days=-1).to_date_string()}.json")
            # today_users = read_users_from_json(f"data/users-day-0.66/{dt.add(days=-1).to_date_string()}.json")
            union_users_dict = union_users_from_yesterday_and_today(yesterday_users, today_users)
            yesterday_users = union_users_dict  # Today will be the yesterday.
            if save_users and dt.day_of_week == 1:
                write_union_users_json(union_users_dict, f"users-cumFrom{super_start_month}", dt.to_date_string())

        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    pd_rsts = pd.DataFrame(rsts).set_index("dt")
    pd_rsts.index = pd.to_datetime(pd_rsts.index)
    pd_rsts = pd_rsts.rename(columns={0: "Biden", 1: "Trump", 2: "Undecided"})
    pd_rsts.to_csv(f"data/csv/cumFrom{super_start_month}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    # pd_rsts.to_csv(f"data/csv/results-cumFrom{super_start_month}-from-{start.to_date_string()}-to-{end.to_date_string()}-0.66.csv")
    

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


def read_located_users():
    users = []
    user_ids = set()
    for line in open("data/located_users.lj"):
        u = json.loads(line.strip())
        _uid = str(u["user_id"])
        if u["user_id"] not in user_ids:
            users.append({"uid": _uid, "state": u["State"]})
            user_ids.add(_uid)
    for line in open("data/located_users_Jan_March.lj"):
        u = json.loads(line.strip())
        _uid = str(u["user_id"])
        if u["user_id"] not in user_ids:
            users.append({"uid": _uid, "state": u["State"]})
            user_ids.add(_uid)
    return pd.DataFrame(users).set_index("uid")


def predict_from_location(start, end, out_dir, save_users=False):
    # df_user = load_df_user_loc(end)
    df_user = read_located_users()
    print(df_user["state"].value_counts())
    df_state_user = {}
    df_state_user["USA"] = set(df_user.index)
    for _s in US_states:
        uid_in_s = set(df_user[df_user.state == _s].index)
        df_state_user[_s] = uid_in_s

    rsts = []
    for dt in pendulum.period(start, end):
        if dt == start or dt.day_of_week != 1:
            continue
        print("Date >", dt)
        json_file = f"data/users-{out_dir}/{dt.to_date_string()}.json"
        users_dict = read_users_from_json(json_file)
        # country
        uid_in_s = df_state_user["USA"]
        users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
        print("USA", len(uid_in_s), len(users_in_s))  # 州，该州多少用户，命中多少用户
        rst = get_share_from_users_dict(users_in_s)
        rst["id"] = "USA:" + dt.to_date_string()
        rst["dt"] = dt.to_date_string()
        rst["state"] = "USA"
        print(rst)
        rsts.append(rst)

        if dt.day_of_week == 1 and save_users:
            write_union_users_json(users_dict, out_dir + "_loc", dt.to_date_string() + "-" + _s)

        # 选择每个洲的结果
        for _s in US_states:
            uid_in_s = df_state_user[_s]
            users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
            print(_s, len(uid_in_s), len(users_in_s))  # 州，该州多少用户，命中多少用户
            rst = get_share_from_users_dict(users_in_s)
            rst["id"] = _s + ":" + dt.to_date_string()
            rst["dt"] = dt.to_date_string()
            rst["state"] = _s
            print(rst)
            rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("id")
    rsts.to_csv(f"data/csv/states-{out_dir}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")

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
    file_name_tweets_prediction = [
        # "data/202009-tweets-prediction-v2.txt",
        # "data/202008-tweets-prediction-v2.txt",
        # "data/202007-tweets-prediction-v2.txt",
        # "data/202006-tweets-prediction-v2.txt",
        # "data/202005-tweets-prediction-v2.txt",
        "data/202004-tweets-prediction-v2.txt",
        "data/202003-tweets-prediction-v2.txt",
        "data/202002-tweets-prediction-v2.txt",
        "data/202001-tweets-prediction-v2.txt",
    ]
    save_user_snapshot_json(file_name_tweets_prediction)

    # start = pendulum.datetime(2020, 1, 1, tz="UTC")
    # end = pendulum.datetime(2020, 6, 1, tz="UTC")
    # sess = get_session_2()
    # # -- to database --
    # # tweets_to_db(sess, start, end, clear=True)             
    # # -- save users' snapshot --
    # save_user_csv(sess, start, end)
    # sess.close()

    # run it per day
    # daily_prediction()

    # -- window start --
    # 1 day
    # start = pendulum.datetime(2020, 1, 8, tz="UTC")
    # end = pendulum.datetime(2020, 7, 19, tz="UTC")
    # calculate_window_share_size_1(start, end, save_csv=True)

    # 7 days
    # start = pendulum.datetime(2020, 1, 8, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=7, save_csv=True)

    # 14 days
    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 10, 20, tz="UTC")
    # calculate_window_share(start, end, win=14)
    # -- window end --

    # -- cumulative start --
    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 10, 20, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="01", save_users=True)
    # -- cumulative end --

    # for states
    # start = pendulum.datetime(2020, 1, 1, tz="UTC")
    # end = pendulum.datetime(2020, 10, 15, tz="UTC")
    # predict_from_location(start, end, out_dir="14days", save_users=True)

    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 10, 15, tz="UTC")
    # predict_from_location(start, end, out_dir="cumFrom01", save_users=False)

    # start = pendulum.datetime(2020, 6, 2, tz="UTC")
    # end = pendulum.datetime(2020, 10, 10, tz="UTC")
    # predict_from_location(start, end, out_dir="cumFrom06", save_users=True)

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

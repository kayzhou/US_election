# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_from_db.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/19 04:01:00 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/26 06:22:05 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *

# ============================================================================
# for demo 2020-02-14

def save_user_snapshot_perday(sess, now):
    users = {}
    for t in tqdm(get_demo_tweets(sess, now, now.add(days=1))):
        uid = t.user_id
        if uid not in users:
            users[uid] = [0, 0, 0, 0, 0, 0]
        users[uid][t.camp] += 1
    # pd.DataFrame(users).T.
    print("# of users:", len(users))
    csv = pd.DataFrame(users).T
    csv.index.names = ['uid']
    csv.to_csv(f"data/users-day/{now.to_date_string()}.csv")


def read_users_from_csv(in_name):
    print("Reading users from csv ...", in_name)
    users = pd.read_csv(in_name).set_index("uid").T.to_dict()
    _users = {}
    for u, v in users.items():
        _users[u] = np.array([v["0"], v["1"], v["2"], v["3"], v["4"], v["5"]])
    print("# of users:", len(_users))
    return _users


def union_users_from_dict(users_groups):
    all_users = {}
    for users in users_groups:
        for u, v in users.items():
            if u not in all_users:
                all_users[u] = v
            else:
                all_users[u] = all_users[u] + v
    return all_users


def union_users_from_yesterday_and_today(yes_users, today_users):
    all_users = yes_users
    for u, v in today_users.items():
        if u not in all_users:
            all_users[u] = v
        else:
            all_users[u] = all_users[u] + v
    return all_users


def write_union_users_csv(union_users_dict, out_dir, dt):
    rsts = []
    if union_users_dict:
        for u, v in union_users_dict.items():
            rst = {
                "uid": u,
                "0": v[0],
                "1": v[1],
                "2": v[2],
                "3": v[3],
                "4": v[4],
                "5": v[5],
            }
            rsts.append(rst)
        pd.DataFrame(rsts).set_index("uid").to_csv(f"disk/{out_dir}/{dt}.csv")
    else:
        with open(f"disk/{out_dir}/{dt}.csv", "w") as f:
            f.write("uid,0,1,2,3,4,5\n")


def get_share_from_users_dict(users_dict):
    counts = {
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0
    }
    for u, v in users_dict.items():
        counts[v.argmax()] += 1
    return counts


def get_share_from_csv(csv_name):
    users_dict = read_users_from_csv(csv_name)
    return get_share_from_users_dict(users_dict)


def calculate_window_share(start, end, win=14, save_csv=None):
    rsts = []
    users_cache = {}

    for dt in pendulum.period(start, end):
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
    rsts = pd.DataFrame(rsts).set_index("dt")
    if save_csv:
        rsts.to_csv(f"data/csv/results-{win}days-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    return rsts


def calculate_cumulative_share(start, end, super_start_month="01", save_csv=None):
    rsts = []
    # from super_start (include) to -1
    if super_start_month == "09":
        super_start = pendulum.datetime(2019, 9, 2, tz="UTC")
    elif super_start_month == "11":
        super_start = pendulum.datetime(2019, 11, 1, tz="UTC")
    elif super_start_month == "01":
        super_start = pendulum.datetime(2020, 1, 1, tz="UTC")
    elif super_start_month == "0115":
        super_start = pendulum.datetime(2020, 1, 15, tz="UTC")
    elif super_start_month == "0201":
        super_start = pendulum.datetime(2020, 2, 1, tz="UTC")
    elif super_start_month == "0215":
        super_start = pendulum.datetime(2020, 2, 15, tz="UTC")
        
    # super_start = start
    yesterday_users = None

    for dt in pendulum.period(start, end):

        if dt <= super_start:
            assert("Error: start <= super_start of cumulative prediction.")
            return -1

        elif dt == super_start.add(days=1):
            union_users_dict = read_users_from_csv(
                f"data/users-day/{super_start.to_date_string()}.csv")
            write_union_users_csv(
                union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())

        else:
            # just from the cumulative yesterday
            # So I must have the yesterday's cumulative csv
            if yesterday_users is None:
                print("Loading yesterday users' csv at", dt.add(days=-1))
                yesterday_users = read_users_from_csv(
                    f"disk/users-culFrom{super_start_month}/{dt.add(days=-1).to_date_string()}.csv")
            today_users = read_users_from_csv(
                f"data/users-day/{dt.add(days=-1).to_date_string()}.csv")
            union_users_dict = union_users_from_yesterday_and_today(
                yesterday_users, today_users)
            yesterday_users = union_users_dict

        # if dt == end:
        election_days = [
            pendulum.datetime(2020, 2, 3, tz="UTC"), # IA
            pendulum.datetime(2020, 2, 11, tz="UTC"), # NH
            pendulum.datetime(2020, 2, 22, tz="UTC"), # NV
        ]
        if dt in election_days :
            print("Writing cumulative users' csv at", dt)
            write_union_users_csv(
                union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())
        
        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("dt")
    
    if save_csv:    
        rsts.to_csv(f"data/csv/results-culFrom{super_start_month}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    
    return rsts
    

def load_df_user_loc():
    print("Loading df_user_loc ...")
    df_users = pd.read_csv("disk/users-location/2020-02-24.csv", 
                           usecols=["uid", "state"]).set_index("uid")
    return df_users


US_states = ['NY', 'DC', 'IN', 'AR', 'WY', 'ME', 'TX', 'NH', 'CO', 'CA', 'IL',
             'WA', 'VA', 'FL', 'MA', 'OR', 'AZ', 'MT', 'MN', 'NE', 'TN', 'OH',
             'NJ', 'NV', 'KY', 'UT', 'NC', 'SC', 'PA', 'NM', 'KS', 'GA', 'MI',
             'WI', 'AK', 'MS', 'MD', 'LA', 'HI', 'MO', 'AL', 'CT', 'OK', 'IA',
             'WV', 'RI', 'SD', 'VT', 'ND', 'ID', 'DE']

def predict_from_location_from_csv(csv_file, save_csv=None):
    df_user = load_df_user_loc()
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


def predict_from_location(start, end, out_dir="14days"):
    df_user = load_df_user_loc()
    election_days = [
        pendulum.datetime(2020, 2, 3, tz="UTC"), # IA
        pendulum.datetime(2020, 2, 11, tz="UTC"), # NH
        pendulum.datetime(2020, 2, 22, tz="UTC"), # NV
        # pendulum.datetime(2020, 2, 26, tz="UTC"), # today
    ]
        
    rsts = []
    for dt in pendulum.period(start, end):

        if dt not in election_days:
            continue

        csv_file = f"disk/users-{out_dir}/{dt.to_date_string()}.csv"
        users_dict = read_users_from_csv(csv_file)

        for _s in US_states:
            uid_in_s = set(df_user[df_user.state == _s].index)
            users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
            print(_s, len(uid_in_s), len(users_dict))

            # write_union_users_csv(users_dict, out_dir, _s + ".csv")
            rst = get_share_from_users_dict(users_in_s)
            rst["id"] = _s + ":" + dt.to_date_string()
            rst["dt"] = dt.to_date_string()
            rst["state"] = _s
            print(rst)
            rsts.append(rst)
            
    rsts = pd.DataFrame(rsts).set_index("id")
    # rsts.to_csv(f"data/csv/results-states-{out_dir}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    rsts.to_csv(f"data/csv/results-states-{out_dir}-electionDays.csv")
        

if __name__ == "__main__":
    # -- save users' snapshot --
    # start = pendulum.datetime(2020, 2, 23, tz="UTC")
    # end = pendulum.datetime(2020, 2, 25, tz="UTC")
    # sess = get_session()
    # for dt in pendulum.period(start, end):
    #     print(dt)
    #     save_user_snapshot_perday(sess, dt)
    # sess.close()

    # -- window start --
    # 7 days
    # start = pendulum.datetime(2020, 1, 8, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=7, save_csv=True)
    
    # # 14 days
    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=14, save_csv=True)

    # # 21 days
    # start = pendulum.datetime(2020, 1, 22, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=21, save_csv=True)
    # -- window end --
    
    # -- cumulative start --
    # start = pendulum.datetime(2019, 9, 3, tz="UTC")
    # end = pendulum.datetime(2020, 2, 19, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="09")

    # start = pendulum.datetime(2019, 11, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 19, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="11")

    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="01", save_csv=True)

    # start = pendulum.datetime(2020, 1, 16, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="0115", save_csv=True)

    # start = pendulum.datetime(2020, 2, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="0201", save_csv=True)

    # start = pendulum.datetime(2020, 2, 16, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="0215", save_csv=True)
    
    # -- cumulative end --

    # election day
    # predict_from_location("disk/users-14days/2020-02-03.csv",
    #                       "disk/results-14days-0203-IA.csv")
    # predict_from_location("disk/users-14days/2020-02-11.csv",
    #                       "disk/results-14days-0211-NH.csv")
    # predict_from_location("disk/users-14days/2020-02-22.csv",
    #                       "disk/results-14days-0222-NV.csv")

    start = pendulum.datetime(2020, 1, 15, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="7days")

    start = pendulum.datetime(2020, 1, 15, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="14days")

    start = pendulum.datetime(2020, 1, 22, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="21days")

    start = pendulum.datetime(2020, 1, 15, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="culFrom01")

    start = pendulum.datetime(2020, 1, 15, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="culFrom0115")

    start = pendulum.datetime(2020, 2, 2, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="culFrom0201")
    
    start = pendulum.datetime(2020, 2, 16, tz="UTC")
    end = pendulum.datetime(2020, 2, 26, tz="UTC")
    predict_from_location(start, end, out_dir="culFrom0215")
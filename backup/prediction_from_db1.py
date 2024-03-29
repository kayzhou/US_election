# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_from_db.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/19 04:01:00 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/03/06 23:27:28 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler1 import *

# ========================================
# for demo 2020-02-14

def save_user_snapshot_perday(sess, now):
    users = {}
    for t in tqdm(get_demo_tweets(sess, now, now.add(days=1))):
        uid = t.user_id
        if uid not in users:
            users[uid] = [0, 0,0]
        users[uid][t.camp] += 1
    # pd.DataFrame(users).T.
    print("# of users:", len(users))
    csv = pd.DataFrame(users).T
    csv.index.names = ['uid']
    csv.to_csv(f"data/users-day_after_BT_m2/{now.to_date_string()}.csv")


def read_users_from_csv(in_name):
    print("Reading users from csv ...", in_name)
    users = pd.read_csv(in_name).set_index("uid").T.to_dict()
    _users = {}
    for u, v in users.items():
        _users[u] = np.array([v["0"], v["1"], v["2"]])
        #_users[u] = np.array([v["0"], v["1"], v["2"], v["3"], v["4"], v["5"]])
    print("# of users:", len(_users))
    return _users


def read_users_from_csv_from_uids(in_name, set_uids):
    print("Reading users from csv ...", in_name)
    users = pd.read_csv(in_name).set_index("uid").T.to_dict()
    _users = {}
    for u, v in users.items():
        if u in set_uids:
            _users[u] = np.array([v["0"], v["1"], v["2"]])
	    # _users[u] = np.array([v["0"], v["1"], v["2"], v["3"], v["4"], v["5"]])
            # _users[u] = np.array([v["1"], v["2"], v["3"], v["4"], v["5"]])
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
    print("Writing ...", f"disk/{out_dir}/{dt}.csv")
    rsts = []
    if union_users_dict:
        for u, v in union_users_dict.items():
            rst = {
                "uid": u,
                "0": v[0],
                "1": v[1],
                "2": v[2],
                #"3": v[3],
                #"4": v[4],
                #"5": v[5],
            }
            rsts.append(rst)
        pd.DataFrame(rsts).set_index("uid").to_csv(f"disk/{out_dir}/{dt}.csv")
    else:
        with open(f"disk/{out_dir}/{dt}.csv", "w") as f:
            f.write("uid,0,1,2\n")
	    #f.write("uid,0,1,2,3,4,5\n")


USERS = pd.read_csv("disk/users-face/2020-03-15.csv").set_index("uid")
USERS_STATE = pd.read_csv("disk/users-location/2020-03-15.csv", 
                          usecols=["uid","state"],
                          error_bad_lines=False).set_index("uid")
USERS_STSTE_GENDER_AGE = USERS.join(USERS_STATE, how="inner")
SET_USERS = set(USERS_STSTE_GENDER_AGE.index)


def write_union_users_csv_v2(union_users_dict, out_dir, dt):
    num2label = {
        #0: "Pete Buttigieg", 
        0: "Bernie Sanders", 
        #2: "Elizabeth Warren", 
        1: "Joe Biden", 
        2: "Others",
        #5: "Mike Bloomberg"
    }
    print("Writing ...", f"disk/{out_dir}/{dt}.csv")
    rsts = []
    if union_users_dict:
        for u, v in union_users_dict.items():
            if u in SET_USERS:
                max_i = v.argmax()
                rsts.append({"uid": u, "Camp": num2label[max_i]})
        
        _users = pd.DataFrame(rsts).set_index("uid").join(USERS_STSTE_GENDER_AGE, how="inner")
        _users.to_csv(f"disk/{out_dir}/{dt}.csv")


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
        max_i = v.argmax()
        counts[max_i] += 1
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
        super_start = pendulum.datetime(2020, 2, 19, tz="UTC")
    elif super_start_month == "0912":
        super_start = pendulum.datetime(2019, 9, 12, tz="UTC")
    elif super_start_month == "0917":
        super_start = pendulum.datetime(2019, 9, 17, tz="UTC")
    elif super_start_month == "0922":
        super_start = pendulum.datetime(2019, 9, 22, tz="UTC")
    elif super_start_month == "0302":
        super_start = pendulum.datetime(2020, 3, 2, tz="UTC")
    elif super_start_month == "0302":
        super_start = pendulum.datetime(2020, 3, 2, tz="UTC")

    # super_start = start
    yesterday_users = None

    for dt in pendulum.period(start, end):
        print(dt)

        if dt <= super_start:
            assert("Error: start <= super_start of cumulative prediction.")
            return -1

        elif dt == super_start.add(days=1):
            union_users_dict = read_users_from_csv(
                f"data/users-day_after_BT_m3/{super_start.to_date_string()}.csv")
            print("Writing the first!")
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
                f"data/users-day_after_BT_m3/{dt.add(days=-1).to_date_string()}.csv")
            union_users_dict = union_users_from_yesterday_and_today(
                yesterday_users, today_users)
            yesterday_users = union_users_dict
            #Matteo modifed here
            #if dt == end: 
            #    print("Writing cumulative users' csv at", dt)
            write_union_users_csv_v2(
                    union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())
        
        rst = get_share_from_users_dict(union_users_dict)
        rst["dt"] = dt.to_date_string()
        print(rst)
        rsts.append(rst)

    rsts = pd.DataFrame(rsts).set_index("dt")
    
    if save_csv:    
        rsts.to_csv(f"data/csv/results-culFrom{super_start_month}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")
    return rsts


def calculate_t0_share(start, super_end, save_csv=None):
    rsts = []
        
    # super_start = start
    next_users = None

    for i, dt in enumerate(pendulum.period(super_end, start)):
        print(i, dt)

        if dt == super_end:
            union_users_dict = read_users_from_csv(
                f"data/users-day_after_BT_m2/{super_end.add(days=-1).to_date_string()}.csv")
            write_union_users_csv(
                union_users_dict, f"users-To{super_end.to_date_string()}", "1")

        else:
            if next_users is None:
                print("Loading next users' csv.")
                next_users = read_users_from_csv(f"disk/users-To{super_end.to_date_string()}/1.csv")

            today_users = read_users_from_csv(
                f"data/users-day_after_BT_m2/{dt.add(days=-1).to_date_string()}.csv")
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

    

US_states = ['NY', 'DC', 'IN', 'AR', 'WY', 'ME', 'TX', 'NH', 'CO', 'CA', 'IL',
             'WA', 'VA', 'FL', 'MA', 'OR', 'AZ', 'MT', 'MN', 'NE', 'TN', 'OH',
             'NJ', 'NV', 'KY', 'UT', 'NC', 'SC', 'PA', 'NM', 'KS', 'GA', 'MI',
             'WI', 'AK', 'MS', 'MD', 'LA', 'HI', 'MO', 'AL', 'CT', 'OK', 'IA',
             'WV', 'RI', 'SD', 'VT', 'ND', 'ID', 'DE']


def load_df_user_loc(dt):
    print("Loading df_user_loc ...")
    df_users = pd.read_csv(f"disk/users-location/{dt.to_date_string()}.csv", 
                           usecols=["uid", "state"]).set_index("uid")
    return df_users


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


def predict_from_location(start, end, out_dir="40days", save_csv=True):
    df_user = load_df_user_loc(end)
        
    rsts = []
    for dt in pendulum.period(start, end):
        if dt == start:
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

    if save_csv:
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
        "IA": pendulum.datetime(2020, 2, 1, tz="UTC"), # IA
        "NH": pendulum.datetime(2020, 2, 3, tz="UTC"), # NH
        # "NV": pendulum.datetime(2020, 1, 22, tz="UTC"), # NV
    }

    super_start = superStart_of_states[state]
    # super_start = start
    yesterday_users = None

    pathlib.Path(f"disk/users-From{super_start.to_date_string()}-{state}").mkdir(exist_ok=True) 

    rsts = []
    for dt in pendulum.period(start, end):

        if dt <= super_start: # start in the function is always the next day of super_start
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
    rsts.to_csv(f"data/csv/results-From{super_start.to_date_string()}-{state}-from-{start.to_date_string()}-to-{end.to_date_string()}.csv")


def daily_prediction():
    end = pendulum.today(tz="UTC") # not include this date
    start = pendulum.yesterday(tz="UTC") # include this date
    
    # nation From01
    calculate_cumulative_share(start, end, super_start_month="01")

    # states 40days
    calculate_window_share(start, end, win=40)
    predict_from_location(start, end, out_dir="40days")

    
if __name__ == "__main__":
    # -- save users' snapshot --
    #start = pendulum.datetime(2020, 3, 6, tz="UTC")
    #end = pendulum.datetime(2020, 3, 11, tz="UTC")
    #sess = get_session()
    #for dt in pendulum.period(start, end):
    #     print(dt)
    #     save_user_snapshot_perday(sess, dt)
    #sess.close()

    # run it per day
    # daily_prediction()
    
    # -- window start --
    # 7 days
    # start = pendulum.datetime(2020, 1, 8, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=7, save_csv=True)
    
    # # 14 days
    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # calculate_window_share(start, end, win=14, save_csv=True)

    # 21 days
    # start = pendulum.datetime(2020, 1, 22, tz="UTC")
    # end = pendulum.datetime(2020, 2, 28, tz="UTC")
    # calculate_window_share(start, end, win=21, save_csv=True)

    # 40 days
    # start = pendulum.datetime(2020, 2, 27, tz="UTC")
    # end = pendulum.datetime(2020, 2, 29, tz="UTC")
    # calculate_window_share(start, end, win=40, save_csv=True)
    # -- window end --
    
    # -- cumulative start --
    # start = pendulum.datetime(2019, 9, 13, tz="UTC")
    # end = pendulum.datetime(2020, 3, 2, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="0912")
    
    ##Matteo Change
    #start = pendulum.datetime(2019, 9, 3 , tz="UTC")
    #end = pendulum.datetime(2020, 3, 11, tz="UTC")
    #calculate_cumulative_share(start, end, super_start_month="09")

    # start = pendulum.datetime(2020, 20, 19, tz="UTC")
    # end = pendulum.datetime(2020, 3, 6, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="0922")
    
    # start = pendulum.datetime(2019, 11, 1, tz="UTC")
    # end = pendulum.datetime(2020, 3, 2, tz="UTC")
    # calculate_cumulative_share(start, end, super_start_month="11")

    # start = pendulum.datetime(2020, 1, 1, tz="UTC")
    # end = pendulum.datetime(2020, 3, 2, tz="UTC")
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

    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="7days")

    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="14days")

    # start = pendulum.datetime(2020, 1, 22, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="21days")

    # start = pendulum.datetime(2020, 2, 11, tz="UTC")
    # end = pendulum.datetime(2020, 2, 29, tz="UTC")
    # predict_from_location(start, end, out_dir="40days")

    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 3, 2, tz="UTC")
    # predict_from_location(start, end, out_dir="culFrom01")

    # start = pendulum.datetime(2020, 1, 15, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="culFrom0115")

    # start = pendulum.datetime(2020, 2, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="culFrom0201")
    
    # start = pendulum.datetime(2020, 2, 16, tz="UTC")
    # end = pendulum.datetime(2020, 2, 26, tz="UTC")
    # predict_from_location(start, end, out_dir="culFrom0215")

    # t0
    start = pendulum.datetime(2019, 9, 3, tz="UTC")
    end = pendulum.datetime(2020, 3, 11, tz="UTC")
    calculate_t0_share(start, end, save_csv=True)

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

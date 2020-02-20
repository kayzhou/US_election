# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    prediction_from_db.py                              :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/19 04:01:00 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/19 20:04:18 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *

# ============================================================================
# for demo 2020-02-14

def save_user_snapshot_perday(sess, now):
    users = {}
    for t in get_demo_tweets(sess, now, now.add(days=1)):
        uid = t.user_id
        if uid not in users:
            users[uid] = [0, 0, 0, 0, 0]
        users[uid][t.camp] += 1
    # pd.DataFrame(users).T.
    csv = pd.DataFrame(users).T
    csv.index.names = ['uid']
    csv.to_csv(f"disk/users-day/{now.to_date_string()}.csv")


def read_users_from_csv(in_name):
    print("Reading users from csv ...", in_name)
    users = pd.read_csv(in_name).set_index("uid").T.to_dict()
    _users = {}
    for u, v in users.items():
        _users[u] = np.array([v["0"], v["1"], v["2"], v["3"], v["4"]])
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
    for u, v in union_users_dict.items():
        rst = {
            "uid": u,
            "0": v[0],
            "1": v[1],
            "2": v[2],
            "3": v[3],
            "4": v[4]
        }
        rsts.append(rst)
    pd.DataFrame(rsts).set_index("uid").to_csv(f"disk/{out_dir}/{dt}.csv")


def get_share_from_users_dict(dt, users_dict):
    counts = {
        "dt": dt.to_date_string(),
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 0
    }
    for u, v in users_dict.items():
        counts[v.argmax()] += 1
    return counts


def calculate_window_share(start, end):
    rsts = []
    # actually, its from -15 to -1
    # w = 14
    for dt in pendulum.period(start, end):
        # print(dt)
        win_dts = pendulum.period(dt.add(days=-15), dt.add(days=-1))
        users_groups = [read_users_from_csv(
            f"disk/users-day/{win_dt.to_date_string()}.csv") for win_dt in win_dts]
        union_users_dict = union_users_from_dict(users_groups)
        write_union_users_csv(
            union_users_dict, "users-14days", dt.to_date_string())
        rst = get_share_from_users_dict(dt, union_users_dict)
        print(rst)
        rsts.append(rst)
    rsts = pd.DataFrame(rsts).set_index("dt")
    rsts.to_csv(f"disk/results-14days.csv")


def calculate_cumulative_share(start, end, super_start_month):
    rsts = []
    # from super_start (include) to -1
    if super_start_month == "09":
        super_start = pendulum.datetime(2019, 9, 2, tz="UTC")
    if super_start_month == "11":
        super_start = pendulum.datetime(2019, 11, 1, tz="UTC")
    if super_start_month == "01":
        super_start = pendulum.datetime(2020, 1, 1, tz="UTC")

    # super_start = start
    yesterday_users = None

    for dt in pendulum.period(start, end):

        if dt <= super_start:
            assert("Error: start <= super_start of cumulative prediction.")
            return -1

        elif dt == super_start.add(days=1):
            union_users_dict = read_users_from_csv(
                f"disk/users-day/{super_start.to_date_string()}.csv")
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
                f"disk/users-day/{dt.add(days=-1).to_date_string()}.csv")
            union_users_dict = union_users_from_yesterday_and_today(
                yesterday_users, today_users)
            yesterday_users = union_users_dict

        if dt == end:
        # if dt >= pendulum.datetime(2020, 2, 1, tz="UTC"):
            print("Writing cumulative users' csv at", dt)
            write_union_users_csv(
                union_users_dict, f"users-culFrom{super_start_month}", dt.to_date_string())
        rst = get_share_from_users_dict(dt, union_users_dict)
        print(rst)
        rsts.append(rst)
    rsts = pd.DataFrame(rsts).set_index("dt")
    rsts.to_csv(f"disk/results-culFrom{super_start_month}.csv")


def load_df_user_loc():
    print("Loading df_user_loc ...")
    df_users = pd.read_csv("disk/02-15-user-location.csv", 
                           usecols=["uid", "state"]).set_index("uid")
    return df_users


US_states = ['NY', 'DC', 'IN', 'AR', 'WY', 'ME', 'TX', 'NH', 'CO', 'CA', 'IL',
             'WA', 'VA', 'FL', 'MA', 'OR', 'AZ', 'MT', 'MN', 'NE', 'TN', 'OH',
             'NJ', 'NV', 'KY', 'UT', 'NC', 'SC', 'PA', 'NM', 'KS', 'GA', 'MI',
             'WI', 'AK', 'MS', 'MD', 'LA', 'HI', 'MO', 'AL', 'CT', 'OK', 'IA',
             'WV', 'RI', 'SD', 'VT', 'ND', 'ID', 'DE']


def get_share_from_users_dict_state(state, users_dict):
    counts = {
        "state": state,
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 0
    }
    for u, v in users_dict.items():
        counts[v.argmax()] += 1
    return counts


def predict_from_location(csv_file, out_name):
    df_user = load_df_user_loc()
    users_dict = read_users_from_csv(csv_file)
    rsts = []
    for _s in US_states:
        uid_in_s = set(df_user[df_user.state == _s].index)
        users_in_s = {u: v for u, v in users_dict.items() if u in uid_in_s}
        print(_s, len(uid_in_s), len(users_dict))
        # write_union_users_csv(users_dict, out_dir, _s + ".csv")
        rst = get_share_from_users_dict_state(_s, users_in_s)
        print(rst)
        rsts.append(rst)
    rsts = pd.DataFrame(rsts).set_index("state")
    rsts.to_csv(out_name)


if __name__ == "__main__":
    # save user snapshot
    start = pendulum.datetime(2019, 2, 10, tz="UTC")
    end = pendulum.datetime(2020, 2, 18, tz="UTC")
    sess = get_session()
    for dt in pendulum.period(start, end):
        print(dt)
        save_user_snapshot_perday(sess, dt)
    sess.close()

    start = pendulum.datetime(2019, 2, 11, tz="UTC")
    end = pendulum.datetime(2020, 2, 19, tz="UTC")
    calculate_window_share(start, end)

    # start = pendulum.datetime(2019, 9, 3, tz="UTC")
    # end = pendulum.datetime(2020, 2, 10, tz="UTC")
    calculate_cumulative_share(start, end, super_start_month="09")

    # start = pendulum.datetime(2019, 11, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 10, tz="UTC")
    calculate_cumulative_share(start, end, super_start_month="11")

    # start = pendulum.datetime(2020, 1, 2, tz="UTC")
    # end = pendulum.datetime(2020, 2, 10, tz="UTC")
    calculate_cumulative_share(start, end, super_start_month="01")

    predict_from_location("disk/users-culFrom09/2020-02-19.csv",
                          "disk/results-culFrom09-0219-in-states.csv")
    predict_from_location("disk/users-culFrom11/2020-02-19.csv",
                          "disk/results-culFrom11-0219-in-states.csv")
    predict_from_location("disk/users-culFrom01/2020-02-19.csv",
                          "disk/results-culFrom01-0219-in-states.csv")

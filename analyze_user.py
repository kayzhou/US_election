# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/24 01:48:04 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/05/01 18:35:35 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from read_raw_data import *
import os

def read_users_set(in_name):
    print("Reading set of", in_name, "...")
    return {json.loads(line)["id"] for line in open(in_name)}


def write_users(start, end, set_users_before, out_name):
    with open(out_name, "w") as f:
        for u in read_user_profile(start, end, set_users_before):
            # only users with locations
            u = {
                "id": u["id"],
                "location": u["location"],
                "profile_image_url": u["profile_image_url"],
                "screen_name": u["screen_name"],
                # "name": u["name"],
                # "verified": u["verified"],
                # "description": u["description"],
            }
            f.write(json.dumps(u, ensure_ascii=False) + "\n")


def write_users_fast(set_users_before, out_name):
    from read_raw_data import read_user_profile_fast

    with open(out_name, "w") as f:
        for u in read_user_profile_fast(set_users_before):
            # only users with locations
            u = {
                "id": u["id"],
                "location": u["location"],
                "profile_image_url": u["profile_image_url"],
                "screen_name": u["screen_name"],
                # "name": u["name"],
                # "verified": u["verified"],
                # "description": u["description"],
            }
            f.write(json.dumps(u, ensure_ascii=False) + "\n")


def union_files(in_name1, in_name2, out_name):
    with open(out_name, "w") as f:
        for line in open(in_name1):
            f.write(line)
        for line in open(in_name2):
            f.write(line)


def write_users_today(dt):
    # 首先遍历所有用户1月~6月，之后再每日产生新的
    start = last_dt = dt.add(days=-1)
    end = dt
    set_users_before = read_users_set(f"disk/users-profile/{last_dt.to_date_string()}.lj")
    out_name = f"disk/users-profile/{start.to_date_string()}-{end.to_date_string()}.lj"
    write_users(start, end, set_users_before, out_name)
    in_name2 = out_name
    union_files(f"disk/users-profile/{last_dt.to_date_string()}.lj",
                in_name2, f"disk/users-profile/{end.to_date_string()}.lj"
    )


if __name__ == '__main__':
    # set_users_before = read_users_set("disk/users-profile/2020-04-24.lj")
    set_users_before = None
    start = pendulum.datetime(2020, 1, 1, tz="UTC")
    end = pendulum.datetime(2020, 5, 1, tz="UTC")
    write_users(start, end, set_users_before, "disk/users-profile/2020-01-01-2020-04-30.lj")
    # write_users_fast(set_users_before, "disk/users-profile/2019-09-01-2020-03-01.lj")
    

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user.py                                    :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/02/24 01:48:04 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/24 07:56:56 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from read_raw_data import *

def read_users_set(in_name):
    print("reading", in_name, "...")
    return {json.loads(line)["id"] for line in open(in_name)}


def write_users(start, end, in_name_users_before, out_name):
    set_users_before = read_users_set(in_name_users_before)
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


if __name__ == '__main__':
    start = pendulum.datetime(2020, 2, 14, tz="UTC")
    end = pendulum.datetime(2020, 2, 24, tz="UTC")

    out_name = f"disk/users-profile/{start.to_date_string()}-{end.to_date_string()}.lj"
    write_users(start, end, "disk/02-15-user-profile.lj", out_name)
    

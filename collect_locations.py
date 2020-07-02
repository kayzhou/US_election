# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    collect_locations.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/06/28 16:35:32 by Zhenkun           #+#    #+#              #
#    Updated: 2020/06/28 16:35:32 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from geopy.geocoders import GoogleV3, Nominatim, Bing
from collections import Counter
from my_weapon import *
from multiprocessing.dummy import Pool as ThreadPool


def load_location_mapping(end_name=""):
    loc_to_loc = json.load(open("data/loc-to-loc" + end_name + ".json"))
    loc_to_state = json.load(open("data/loc-to-state" + end_name + ".json"))
    loc_to_county = json.load(open("data/loc-to-county" + end_name + ".json"))
    return loc_to_loc, loc_to_state, loc_to_county


def save_location_mapping(loc_to_loc, loc_to_state, loc_to_county, end_name=""):
    if loc_to_loc:
        json.dump(loc_to_loc,
                  open("data/loc-to-loc" + end_name + ".json", "w"),
                  ensure_ascii=False, indent=2)
    if loc_to_state:
        json.dump(loc_to_state,
                  open("data/loc-to-state" + end_name + ".json", "w"),
                  ensure_ascii=False,
                  indent=2)
    if loc_to_county:
        json.dump(loc_to_county,
                  open("data/loc-to-county" + end_name + ".json", "w"),
                  ensure_ascii=False,
                  indent=2)


def query_from_geolocator(in_name):
    # 获取所有位置信息
    all_locs = Counter()
    for line in tqdm(open(in_name)):
        if not line.startswith("##-"):
            w = line.strip().split("\t")
            try:
                all_locs[w[0]] = int(w[1])
            except Exception as e:
                print(e, w)

    loc_to_loc, loc_to_state, loc_to_county = load_location_mapping("-20200604")
    most_locs = [_loc for _loc in all_locs if _loc not in loc_to_loc and all_locs[_loc] > 10]
    print("Running:", len(most_locs))

    geolocator = Bing(api_key = "AmBIW_84Ow2Dx-JXRlkVjsJVSxvM0eqhTNmK8Y5JCx9UP-PqARFZQi6xtbApJ4cz")
    could_not_find = set()
    # 解析位置到地址
    def get_address(_loc):
        if _loc not in loc_to_loc:
            try:
                addr = geolocator.geocode(_loc)
                loc_to_loc[_loc] = addr.address
            except AttributeError:
                could_not_find.add(_loc)
            except Exception as e:
                print(e)

    pool = ThreadPool(4)
    print("Querying address from BingMap ...")
    pool.map(get_address, most_locs)
    pool.close()
    pool.join()
    print("Could not find:", len(could_not_find))

    states_count = Counter()
    for loc, n in all_locs.most_common():
        if loc in loc_to_loc:
            w = loc_to_loc[loc].split(", ")
            if w[-1] != "United States" or len(w) < 2:
                continue
            # print(w)
            state = w[-2]
            if state == "New York":
                state = "NY"
            # deal with ``IA 50003``
            if len(state) != 2:
                _loc = state.split()
                if len(_loc[0]) == 2:
                    state = _loc[0]
            if len(state) != 2:
                continue

            states_count[state] += n
            loc_to_state[loc] = state

    county_count = Counter()
    for loc, n in all_locs.most_common():
        if loc in loc_to_loc:
            w = loc_to_loc[loc].split(", ")
            if w[-1] != "United States" or len(w) < 3:
                continue
            # print(w)
            county, state = w[-3], w[-2]
            county_count[county] += n
            loc_to_county[loc] = county + ", " + state

    save_location_mapping(loc_to_loc, loc_to_state, loc_to_county, "-20200622")


if __name__ == "__main__":
    query_from_geolocator("disk/users-location/2020-01-2020-06-stat.txt")

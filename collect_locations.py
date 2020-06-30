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
from my_weapon import *
from tqdm.notebook import tqdm as tqdm

# sns.set(style="darkgrid", font_scale=1.2)

# geolocator = GoogleV3(api_key="AIzaSyBr21hhF3-mTkulgEFPts6rthj5wBTtJjc")
# geolocator = Bing(api_key="AitmxhUwVWhHzR57OTAth8oFN9ZXSkq-k4R5h6OHXlhHhf8WzVsYEGAvIxpuk6IW")
geolocator = Bing(api_key="AmBIW_84Ow2Dx-JXRlkVjsJVSxvM0eqhTNmK8Y5JCx9UP-PqARFZQi6xtbApJ4cz")

def write_locations(in_names, out_name):
    # explain the locations > states and counties
    all_locations = Counter()
    set_users = set()

    for in_name in in_names:
        for line in tqdm(open(in_name)):
            u = json.loads(line)
            _id = u["id"]
            if _id not in set_users:
                set_users.add(_id)
                all_locations[u["loc"].lower().replace("\t", " ").replace("\n", " ")] += 1

    with open(out_name, "w") as f:
        for loc, cnt in all_locations.most_common():
            f.write(f"{loc}\t{cnt}\n")
            
write_locations("data/us2016_users.lj", "data/us2016_loc_stat.txt")


def load_location_mapping():
    loc_to_loc = json.load(open("data/loc-to-loc.json"))
    loc_to_state = json.load(open("data/loc-to-state.json"))
    loc_to_county = json.load(open("data/loc-to-county.json"))
    return loc_to_loc, loc_to_state, loc_to_county


def save_location_mapping(loc_to_loc, loc_to_state, loc_to_county):
    json.dump(loc_to_loc, open("data/loc-to-loc", "w"), ensure_ascii=False, indent=2)
    json.dump(loc_to_state, open("data/loc-to-state", "w"), ensure_ascii=False, indent=2)
    json.dump(loc_to_county, open("data/loc-to-county", "w"), ensure_ascii=False, indent=2)


locations = Counter()
# for line in tqdm(open("disk/users-location/2020-01-01-2020-04-19-stat.txt")):
for line in tqdm(open("data/us2016_loc_stat.txt")):
    if not line.startswith("##-"):
        w = line.strip().split("\t")
        try:
            locations[w[0]] = int(w[1])
        except:
            print(w)

# loc_to_loc = {}



most_locs = [loc for loc in most_locs if loc not in loc_to_loc]
len(most_locs)

# collecting locations' info

from multiprocessing.dummy import Pool as ThreadPool


def get_loc(locs, loc_to_loc):
    could_not_find = set()

    if n in loc_to_loc:
        return 
    else:    
        try:
            loc = geolocator.geocode(n)
            # print(loc)
            loc_to_loc[n] = loc.address
        except AttributeError:
            could_not_find.add(n)
        except Exception as e:
            print(e)

pool = ThreadPool(4)
pool.map(get_address, most_locs)
pool.close()
pool.join()


# loc_to_state = {}
states_count = Counter()

for loc, n in locations.most_common():
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

len(loc_to_state)
# states_count

json.dump(loc_to_state, open("data/loc-to-state-20200604.json", "w"), ensure_ascii=False, indent=2)

# county
# loc_to_county = {}

county_count = Counter()

for loc, n in locations.most_common():
    if loc in loc_to_loc:
        w = loc_to_loc[loc].split(", ")
        if w[-1] != "United States" or len(w) < 3:
            continue
        # print(w)
        county, state = w[-3], w[-2]
        county_count[county] += n
        loc_to_county[loc] = county + ", " + state

len(loc_to_county)

json.dump(loc_to_county, open("data/loc-to-county-20200604.json", "w"), ensure_ascii=False, indent=2)


if __name__ == "__main__":
    
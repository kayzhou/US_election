# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    daily_profile.py                                   :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <kayzhou.mail@gmail.com>          +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2019/05/08 18:48:50 by Kay Zhou          #+#    #+#              #
#    Updated: 2019/09/12 18:47:24 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #

from SQLite_handler import *
from my_faces import analyze_image
from multiprocessing.dummy import Pool as ThreadPool
from geopy.geocoders import GoogleV3

geolocator = GoogleV3(api_key="AIzaSyBr21hhF3-mTkulgEFPts6rthj5wBTtJjc")

## crontab -e
## 0 1 * * * cd /home/alex/kayzhou/Argentina_election; nohup /home/alex/anaconda3/bin/python daily_predict.py >> log.txt 2>&1 & 
"""
class User_Profile(Base):
    __tablename__ = "user_profile"
    user_id = Column(Integer, primary_key=True)
    location = Column(String)
    parsed_location = Column(String)
    country = Column(String)
    age = Column(Integer)
    gender = Column(Integer)
"""

def main():
    start = pendulum.yesterday(tz="UTC") # include this date
    end = pendulum.today(tz="UTC") # not include this date
    print(f"{start} <= run < {end} ")

    sess = get_session()
    users = {}
    for d, dt in read_end_file(start, end):
        u = d["user"]
        if u["id"] in users:
            continue
        if "location" in u and not sess.query(exists().where(User_Profile.user_id == u["id"])).scalar():
            users[u["id"]] = u

    sess.close()
    print("all users with locations:", len(users))
    # print(users)
    
    # locations
    print("Analyzing locations ...")
    loc_to_country = json.load(open("data/loc_to_country_v2"))

    locations_info = set([d["location"] for uid, d in users.items()])
    
    def get_address(n):
        if n in loc_to_country:
            return 0
        elif "🇦🇷" in n or "Argentina" in n:
            loc_to_country[n] = "Argentina"
        else:
            try:
                loc = geolocator.geocode(n)
                loc_to_country[n] = loc.address
            except AttributeError:
                pass
            except Exception as e:
                print(e, n)
            
    pool = ThreadPool(4)
    pool.map(get_address, list(locations_info))
    for uid, d in users.items():
        if d["location"] in loc_to_country:
            d["parsed_location"] = loc_to_country[d["location"]]
            d["country"] = d["parsed_location"].split(", ")[-1]

    json.dump(loc_to_country, open("data/loc_to_country_v2", "w"))

    # faces
    print("Analyzing faces ...")
    urls = [(d["profile_image_url_https"], d) for uid, d in users.items()]
    pool = ThreadPool(8)
    rsts = pool.map(analyze_image, urls)

    users = [User_Profile(
        user_id=d["id"],
        location=d["location"],
        parsed_location=d["parsed_location"],
        country=d["country"],
        age=d["faces"][0]['attributes']["age"]["value"],
        gender=d["faces"][0]['attributes']["gender"]["value"]
    ) for d in rsts if "faces" in d and "country" in d]
    print("add new users with profile:", len(users))

    sess = get_session()
    sess.add_all(users)
    sess.close()


if __name__ == "__main__":
    main()
    # pass

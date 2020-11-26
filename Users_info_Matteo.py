import numpy as np
from pathlib import Path
from read_raw_data import *

def users_month_Jan_to_March():
    months = set([
         "202001",
         "202002",
         "202003",
      ])
    file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
    set_users = set()
    with open("/home/alex/kayzhou/US_election/data/users_info_March_April.lj","w") as _file:
        for in_name in file_names:
            word = in_name.stem.split("-")[-1]
            if word in demo_files and in_name.parts[1] in months:
                print(in_name)
                for line in open(in_name):
                    try:
                        d = json.loads(line.strip())
                    except Exception:
                        print("JSON loading error")
                    if d["user"]["id"] in set_users:
                        continue
                    set_users.add(d["user"]["id"])
                    loc,profile_image_url=None,None
                    if 'location' in d['user']:
                        loc=d["user"]["location"]
                    if "profile_image_url" in d["user"]:
                        profile_image_url=d["user"]["profile_image_url"]
                    user_info={"user_id":d["user"]["id"],
                              "screen_name":d["user"]["screen_name"],
                              "location": loc,
                              "profile_image_url":profile_image_url}
                    _file.write(json.dumps(user_info, ensure_ascii=False) + "\n")

if __name__ == '__main__':
    users_month_Jan_to_March()
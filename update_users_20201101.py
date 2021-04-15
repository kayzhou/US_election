from collections import Counter
from pathlib import Path

import pendulum
import ujson as json
from file_read_backwards import FileReadBackwards
from tqdm import tqdm

months = set([
     "202010",
     "202011",
  ])
file_names = sorted(Path("raw_data").rglob("*.txt"), reverse=True)
set_users = set()
for i in open('raw_data/user_info/Final_users_list.lj'):
    set_users.add(json.loads(i)['id'])
with open("raw_data/user_info/Final_users_list.lj","a") as _file:
    for in_name in file_names:
        word = in_name.stem.split("-")[-1]
        if in_name.parts[1] in months and in_name.parts[2].split('-')[0]>'20200911':
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
                user_info={"id":d["user"]["id"],
                          "screen_name":d["user"]["screen_name"],
                          "location": loc,
                          "profile_image_url":profile_image_url}
                _file.write(json.dumps(user_info, ensure_ascii=False) + "\n")

# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user_face.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Zhenkun <zhenkun91@outlook.com>            +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/06/05 22:59:52 by Zhenkun          ###   ########.fr        #
#                                                                              #
# **************************************************************************** #


import simplejson as json
import time
import os
import unicodedata
from multiprocessing.dummy import Pool as Pool

import requests
from tqdm import tqdm
from my_weapon import *
from pathlib import Path
from collections import Counter


api = 'https://api-us.faceplusplus.com/facepp/v3/detect'

MY_KEYS = [
    {
        "key": "dxJ1XG-lII-uhRkgPPyN6KTGlj-OFfPP",
        "secret": "KuZ5yqIzlzg1knufR6qAuP2g37JcIPK2"
    }, {
        "key": "dkphe9CFauJzZyAnlWgbZKAOXnsult27",
        "secret": "eUhcLN--M8RIFwb4Y11UkRUavUfud0aR"
    }, {
        "key": "CpNGTnX-LFtDL8di1JXC48viZr4HuCug",
        "secret": "1gjcS00x-CYiJw4Kx8Kv4w90WaAbNKGt"
    }, {
        "key": "iLAN86FRNcWMsPGRJ--8l_SJz949fHZm",
        "secret": "1uz1TPC-AJupdLaJmArDG_T7_np15x-S"
    }, {
        "key": "OuEuORtuVaBFFc8Z1Jm6ju2fK9QP6HhD",
        "secret": "m-9xFb3FdyZll76WEopW7shKzy-hfTSe",
    }, {
        "key": "vup9dFRsCKiNwlqy2fK6dhdJQ-Dcykjo",
        "secret": "vee64E85L8gAVYdpXo9Z4rGLrR6J42SN",
    }, {
        "key": "3V5qpgMjO-hvDDe4XBuVYH1g33R__5zB",
        "secret": "ieeyVNe-g5x0w8Q9qHu9IgFunIyYu5H1",
    }, {
        "key": "ixwrzUY573M27VcAG84aoK2_u9vgc-ZU",
        "secret": "if-Hz04oX9r-huCX8W-A7nD50r4thB-E",
    }, {
        "key": "ezVU5B_lKUVfrlKGNqV5aXLn3-MZ7Bku",
        "secret": "aR5_sSOESr584ubnXMm2xsN5YA1km5Zm", 
    }, {
        "key": "BlXVdJebV15-tRHacCW3Qbzi7qwqQswH",
        "secret": "oWm0l4wTUXYdmw0-6BPH2mGerZ3PGu6k",
    }
]


def get_clear_picture_url(url):
    return url.replace("_normal", "")


def get_clear_picture_url_200(url):
    return url.replace("_normal", "_200x200")


def get_key():
    global MY_KEYS
    while True:
        for k in MY_KEYS:
            yield k["key"], k["secret"]


KEY_STORE = get_key()

         
def analyze_image(_urls):
    key, secret = next(KEY_STORE)
    url = get_clear_picture_url(_urls[0])
    # url = _urls[0]
    d = _urls[1]
    # data to be sent to api
    data = {
            'api_key': key,
            'api_secret': secret,
            'image_url': url,
            'face_tokens': "",
            'return_landmark': 0,
            'return_attributes': "gender,age"
    }
    for i in range(3):
        try:
            r = requests.post(url=api, data=data).json()
            if "faces" in r and r["faces"]:
                d["faces"] = r["faces"]
                # print(f"succeed! {i}")
                return d

            elif "error_message" in r:
                #print("Normal Error:", data['image_url'])
                if r["error_message"] == "INVALID_IMAGE_URL":
                    d["error_message"] = r["error_message"]
                    return {"id": d["id"], "error_message": d["error_message"]}
                elif r["error_message"] == "CONCURRENCY_LIMIT_EXCEEDED":
                    #print(f'{r["error_message"]}')
                    time.sleep(0.5)
                else:
                    #print("Other error:", r["error_message"])
                    return {"id": d["id"], "error_message": d["error_message"]}
            else:
                # print("No Face~")
                d["faces"] = None
                return d
        except Exception as e:
            #print("Special analyze_image() ERROR:", e)
            return None


def analyze_face(users, out_file):
    """
    in_name > users-profile/*.lj
    For collect_user.py
    """
    print("开始人脸识别：", len(users), "个用户。")
    urls = [(u["profile_image_url"], u) for u in users if not u["profile_image_url"].endswith("gif")]
    pool = Pool(8)
    rsts = pool.map(analyze_image, urls)
    pool.close()
    pool.join()

    for d in rsts:
        if d:
            out_file.write(json.dumps(d) + "\n")

        
def analyze_face_from_file(in_name, out_name, have_name=None):
    """
    in_name > users-profile/*.lj
    """
    urls = []
    cnt = 0
    # bingo = False
    # dt_str = dt.to_date_string()

    all_ids = {json.loads(line)["id"] for line in open(in_name)}
    print(len(all_ids))

    #have_ids = {json.loads(line)["id"] for line in open(have_name)}
    #noFace_ids = {json.loads(line)["id"] for line in open("disk/users-face/noFace.lj")}
    #should_ids = all_ids - have_ids - noFace_ids

    #print("need:", len(should_ids))

    # run it again
    error_file = open(f"{out_name[:-3]}-error.lj", "a")
    # throw it away
    no_face_file = open(f"{out_name[:-3]}_noFace.lj", "a")
    with open(f"{out_name}", "w") as f:
        for line in tqdm(open(in_name)):
            cnt += 1
            d = json.loads(line)
            
            #if d["id"] not in should_ids:
            #    continue

            # print(d)
            url = d["profile_image_url"]
            urls.append((url, d))

            if len(urls) >= 1024:
                # print(cnt)
                pool = Pool(6)
                rsts = pool.map(analyze_image, urls)
                pool.close()
                pool.join()
                for d in rsts:
                    if d is not None:
                        if "faces" in d:
                            f.write(json.dumps(d) + "\n")
                        elif "error_message" in d:
                            error_file.write(json.dumps(d) + "\n")
                        elif "no_face" in d:
                            no_face_file.write(json.dumps(d) + "\n")
                urls = []
                print('analyzed: ', cnt)

        for _url in tqdm(urls):
            d = analyze_image(_url)
            if d is not None:
                if "faces" in d:
                    f.write(json.dumps(d) + "\n")
                elif "error_message" in d:
                    error_file.write(json.dumps(d) + "\n")
                elif "no_face" in d:
                    no_face_file.write(json.dumps(d) + "\n")


def union_files(in_name1, in_name2, out_name):
    with open(out_name, "w") as f:
        for line in open(in_name1):
            f.write(line)
        for line in open(in_name2):
            f.write(line)


def get_users_from_lj(in_name, out_name=None):
    users = []
    
    for line in tqdm(open(in_name)):
        d = json.loads(line.strip())
        face = d["faces"][0]
        # print(face)
        age = face['attributes']["age"]["value"]
        gender = face['attributes']["gender"]["value"]

        age_range = "UNK"
        if age < 18:
            continue
        elif age >= 18 and age < 30:
            age_range = ">=18, <30"
        elif age >= 30 and age < 50:
            age_range = ">=30, <50"
        elif age >= 50 and age < 65:
            age_range = ">=50, <65"
        elif age >= 65:
            age_range = ">=65"

        users.append(
            {
                "uid": d["id"], 
                "age": age, 
                "gender": gender, 
                "age_range": age_range
            }
        )

    users = pd.DataFrame(users).set_index("uid")
    users = users[~users.index.duplicated(keep='first')]

    if out_name:
        users.to_csv(out_name)
        
    return users


def write_users_today_face_csv(dt):
    """
    in_name: users-profile
    out_name: users-location.csv
    """
    start = dt.add(days=-1)
    end = dt
    
    analyze_face_from_file(f"disk/users-profile/{start.to_date_string()}-{end.to_date_string()}.lj",
                           f"disk/users-face/{start.to_date_string()}.lj",
                           out_name=f"{start.to_date_string()}-{end.to_date_string()}")

    union_files(f"disk/users-face/{start.to_date_string()}.lj",
                f"disk/users-face/{start.to_date_string()}-{end.to_date_string()}.lj",
                f"disk/users-face/{end.to_date_string()}.lj"
    )

    get_users_from_lj(f"disk/users-face/{end.to_date_string()}.lj", 
                      out_name=f"disk/users-face/{end.to_date_string()}.csv")


if __name__ == '__main__':
    # write_users_today_face_csv(pendulum.today())
    # write_users_today_face_csv(pendulum.datetime(2020, 3, 12))

    # analyze_face_from_file(f"disk/users-profile/2020-03-05-2020-03-06.lj",
    #                        f"disk/users-face/2020-03-02.lj",
    #                        out_name=f"2020-03-05-2020-03-06")
    
    analyze_face_from_file('data/county_users/County_users_to_analyze.lj',
                           'data/county_users/County_users_analyzed.lj')
                           
    #get_users_from_lj(f"disk/users-face/2020-04-30.new.lj").to_csv(f"disk/users-face/2020-04-30.csv")

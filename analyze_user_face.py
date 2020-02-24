# **************************************************************************** #
#                                                                              #
#                                                         :::      ::::::::    #
#    analyze_user_face.py                               :+:      :+:    :+:    #
#                                                     +:+ +:+         +:+      #
#    By: Kay Zhou <zhenkun91@outlook.com>           +#+  +:+       +#+         #
#                                                 +#+#+#+#+#+   +#+            #
#    Created: 2020/01/21 09:47:55 by Kay Zhou          #+#    #+#              #
#    Updated: 2020/02/24 08:57:17 by Kay Zhou         ###   ########.fr        #
#                                                                              #
# **************************************************************************** #


import simplejson as json
import time
import os
import unicodedata
from multiprocessing.dummy import Pool as ThreadPool

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

key_store = get_key()


def analyze_image(_urls):
    key, secret = next(key_store)
    url = get_clear_picture_url(_urls[0])
    d = _urls[1]
        
    # data to be sent to api 
    data = {
            'api_key': key,
            'api_secret': secret, 
            'image_url': url,
            'face_tokens': "",
            # 'return_landmark': 0,
            'return_attributes': "gender,age,ethnicity"
    }
        
    for i in range(3):
        # sending post request and saving response as response object 
        try:
            r = requests.post(url=api, data=data).json()
            if "faces" in r and r["faces"]:
                d["faces"] = r["faces"]
                # print(f"succeed! {i}")
                return d

            elif "error_message" in r:
                if r["error_message"] == "INVALID_IMAGE_URL":
                    d["error_message"] = r["error_message"]
                    return {"id": d["id"], "error_message": d["error_message"]}
                elif r["error_message"] == "CONCURRENCY_LIMIT_EXCEEDED":
                    print(f'{r["error_message"]}')
                    time.sleep(1)
                else:
                    print("Other error:", r["error_message"])
                    return {"id": d["id"], "error_message": d["error_message"]}
            else:
                # print("No Face~")
                return {"id": d["id"], "no_face": True}
        except Exception as e:
            print("Notice!! ERROR:", e)
            return None
    

def analyze_history_02_15():
    urls = []
    cnt = 0
    # bingo = False
    all_ids = {json.loads(line.strip())["id"] for line in open("disk/02-15-user-profile.lj")}
    print(len(all_ids))

    have_ids = set()
    should_ids = all_ids - have_ids
    print("need:", len(should_ids))

    # run it again
    error_file = open("disk/02-15-user-profile-error.txt", "a")
    # throw it away
    no_face_file = open("disk/02-15-user-profile-noFace.txt", "a")

    with open("disk/02-15-user-profile-face.lj", "w") as f:
        for line in tqdm(open("disk/02-15-user-profile.lj")):
            cnt += 1
            d = json.loads(line.strip())
            
            # if d["id"] not in should_ids:
                # continue

            url = d["profile_image_url_https"]
            urls.append((url, d))

            if len(urls) >= 1024:
                # print(cnt)
                pool = ThreadPool(8)

                # multithread
                rsts = pool.map(analyze_image, urls)

                for d in rsts:
                    if d is not None:
                        if "faces" in d:
                            f.write(json.dumps(d) + "\n")
                        elif "error_message" in d:
                            error_file.write(json.dumps(d) + "\n")
                        elif "no_face" in d:
                            no_face_file.write(json.dumps(d) + "\n")
                urls = []

        for _url in tqdm(urls):
            d = analyze_image(_url)
            if d is not None:
                if "faces" in d:
                    f.write(json.dumps(d) + "\n")
                elif "error_message" in d:
                    error_file.write(json.dumps(d) + "\n")
                elif "no_face" in d:
                    no_face_file.write(json.dumps(d) + "\n")


def analyze_face_from_file(in_name, have_name, out_name):
    """
    in_name > users-profile/*.lj
    """
    urls = []
    cnt = 0
    # bingo = False
    # dt_str = dt.to_date_string()

    all_ids = {json.loads(line)["id"] for line in open(f"disk/users-face/{in_name}")}
    print(len(all_ids))

    have_ids = {json.loads(line)["id"] for line in open(f"disk/users-face/{have_name}")}
    noFace_ids = {json.loads(line)["id"] for line in open("disk/users-face/noFace.lj")}
    should_ids = all_ids - have_ids - noFace_ids

    print("need:", len(should_ids))

    # run it again
    error_file = open(f"disk/users-face/{out_name}-error.lj", "a")
    # throw it away
    no_face_file = open("disk/users-face/noFace.txt", "a")

    with open(f"disk/users-face/{in_name}.lj", "w") as f:
        for line in tqdm(open(in_name)):
            cnt += 1
            d = json.loads()
            
            if d["id"] not in should_ids:
                continue

            url = d["profile_image_url_http"]
            urls.append((url, d))

            if len(urls) >= 1024:
                # print(cnt)
                pool = ThreadPool(4)

                # multithread
                rsts = pool.map(analyze_image, urls)

                for d in rsts:
                    if d is not None:
                        if "faces" in d:
                            f.write(json.dumps(d) + "\n")
                        elif "error_message" in d:
                            error_file.write(json.dumps(d) + "\n")
                        elif "no_face" in d:
                            no_face_file.write(json.dumps(d) + "\n")
                urls = []

        for _url in tqdm(urls):
            d = analyze_image(_url)
            if d is not None:
                if "faces" in d:
                    f.write(json.dumps(d) + "\n")
                elif "error_message" in d:
                    error_file.write(json.dumps(d) + "\n")
                elif "no_face" in d:
                    no_face_file.write(json.dumps(d) + "\n")

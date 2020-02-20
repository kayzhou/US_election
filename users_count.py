#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 11:12:09 2020

@author: Carles
"""
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Names for each candidate
candidates_names = ["Joe Biden", "Mike Bloomberg", "Pete Buttigieg", 
                    "Tulsi Gabbard", "Amy Klobuchar", "Bernie Sanders",
                    "Elizabeth Warren"]

# Queries for each candidate
biden_queries = set([
    "Joe Biden",
    "JoeBiden"
])

bloomberg_queries = set([
    "Mike Bloomberg",
    "MikeBloomberg"
])

buttigieg_queries = set([
    "Pete Buttigieg",
    "PeteButtigieg"
])

gabbard_queries = set([
    "Tulsi Gabbard",
    "TulsiGabbard"
])

klobuchar_queries = set([
    "Amy Klobuchar",
    "amyklobuchar"
])

sanders_queries = set([
    "Bernie Sanders",
    "SenSanders"
])

warren_queries = set([
    "Elizabeth Warren",
    "ewarren"
])

# Dictionary init for each candidate

# Dictionaries for pulling the data
biden_users = {}
bloomberg_users = {}
buttigieg_users = {}
gabbard_users = {}
klobuchar_users = {}
sanders_users = {}
warren_users = {}

# Dictionaries for acumulated users count
biden_accumul = {}
bloomberg_accumul = {}
buttigieg_accumul = {}
gabbard_accumul = {}
klobuchar_accumul = {}
sanders_accumul = {}
warren_accumul = {}

# Dictionaries for daily users count
biden_daily = {}
bloomberg_daily = {}
buttigieg_daily = {}
gabbard_daily = {}
klobuchar_daily = {}
sanders_daily = {}
warren_daily = {}

# Ditionary for translating months into numbers
months = {
        "Jan" : "01",
        "Feb" : "02",
        "Mar" : "03",
        "Apr" : "04",
        "May" : "05",
        "Jun" : "06",
        "Jul" : "07",
        "Aug" : "08",
        "Sep" : "09",
        "Oct" : "10",
        "Nov" : "11",
        "Dec" : "12"
        }

# Function for removing duplicates elements from a list
def remove_duplicates(duplicate, original = None): 
    if original is None:
        original = []
    final_list = original
    for element in duplicate: 
        if element not in final_list: 
            final_list.append(element)
    return final_list 

# Function to count the numbers of tweets in a file
def extract_users(file_name, candidate_users):
    for line in tqdm(open(file_name)):
        tweet_date = json.loads(line)["created_at"].split(" ")
        user = json.loads(line)["user"]["id"]
        if candidate_users.get(tweet_date[-1]+" "+months[tweet_date[1]]+" "+tweet_date[2]) is None:
            candidate_users[tweet_date[-1]+" "+months[tweet_date[1]]+" "+tweet_date[2]] = []
        candidate_users[tweet_date[-1]+" "+months[tweet_date[1]]+" "+tweet_date[2]].append(user)
   
# Function for sorting the data read     
def sort_dictionaries(dictionaries):
    
    # Get all the dates with value and sort them
    all_dates = []
    for dic in dictionaries:
        all_dates.extend(list(dic.keys()))   
    dates = remove_duplicates(all_dates)
    dates.sort()
    
    # Sort all the data by dates
    sorted_dictionaries = []
    for dic in dictionaries:
        sorted_dic = {}
        for d in dates:
            if dic.get(d) is None:
                sorted_dic[d] = []
            else:
                sorted_dic[d] = dic[d]
        sorted_dictionaries.extend([sorted_dic])
        
    return sorted_dictionaries
    
def create_dataframe(dictionaries, names):
    
    dates = list(dictionaries[1].keys())
    
    # Create the data frame
    d = {"Dates" : dates}
    
    for j in range(0,len(names)):
        d[names[j]] = list(dictionaries[j].values())
        
    data_frame = pd.DataFrame(data=d)
    return data_frame
    
# Reading of each file and relate the counting to the canditate
for in_name in Path("raw_data").rglob("*.txt"):
    if in_name.stem.split("-")[-1] in biden_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= biden_users)
    elif in_name.stem.split("-")[-1] in bloomberg_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= bloomberg_users)
    elif in_name.stem.split("-")[-1] in buttigieg_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= buttigieg_users)
    elif in_name.stem.split("-")[-1] in gabbard_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= gabbard_users)
    elif in_name.stem.split("-")[-1] in klobuchar_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= klobuchar_users)
    elif in_name.stem.split("-")[-1] in sanders_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= sanders_users)
    elif in_name.stem.split("-")[-1] in warren_queries:
        print(in_name)
        extract_users(file_name=in_name, candidate_users= warren_users)
       
# Group all the dictionaries to iterate through them
all_users = [biden_users, bloomberg_users, buttigieg_users,
            gabbard_users, klobuchar_users, sanders_users, 
            warren_users] 

accumul_users = [biden_accumul, bloomberg_accumul, buttigieg_accumul,
            gabbard_accumul, klobuchar_accumul, sanders_accumul, 
            warren_accumul]

daily_users = [biden_daily, bloomberg_daily, buttigieg_daily,
            gabbard_daily, klobuchar_daily, sanders_daily, 
            warren_daily] 

# Sort the data by days
print("Sorting the data by date")
all_users = sort_dictionaries(all_users)

# Count the number of unique users per day and acumulated
candidate = 0
accumulated_users = set()
for candidate_users in all_users:
    print("Starting candidate " + candidates_names[candidate] + "'s users count")
    for date, users in candidate_users.items():
        print("Day: "+ date)
        unique_users = set(users)
        accumulated_users.update(unique_users)
        daily_users[candidate][date] = len(unique_users)
        accumul_users[candidate][date] = len(accumulated_users)
    print("Finished candidate " + candidates_names[candidate] + "'s users count")
    accumulated_users = set()
    candidate += 1

# Create and save data frames
print("Creating data frame for daily count")
df_daily = create_dataframe(daily_users, candidates_names)

df_daily.to_csv('data/users_daily_count_test.csv',index = None)
print("Data frame saved in csv file")

print("Creating data frame for cumulative count")
df_cumul = create_dataframe(accumul_users, candidates_names)

df_cumul.to_csv('data/users_cumulative_count_test.csv',index = None)
print("Data frame saved in csv file")
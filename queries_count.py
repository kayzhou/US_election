#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  7 11:12:09 2020

@author: Carles
"""
import json
import pandas as pd
from pathlib import Path
from collections import Counter
from tqdm import tqdm

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

yang_queries = set([
    "Andrew Yang",
    "AndrewYang"
])

# Counter init for each candidate
biden_count = Counter()
bloomberg_count = Counter()
buttigieg_count = Counter()
gabbard_count = Counter()
klobuchar_count = Counter()
sanders_count = Counter()
warren_count = Counter()
yang_count = Counter()

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
def remove_duplicates(duplicate): 
    final_list = [] 
    for element in duplicate: 
        if element not in final_list: 
            final_list.append(element) 
    return final_list 

# Function to count the numbers of tweets in a file
def count_tweets(file_name, candidate_count):
    for line in tqdm(open(file_name)):
        tweet_date = json.loads(line)["created_at"].split(" ")
        candidate_count[tweet_date[-1]+" "+months[tweet_date[1]]+" "+tweet_date[2]] +=1
   
# Function for sorting the data read     
def sort_data(counters, names):
    
    # Get all the dates with value and sort them
    all_dates = []
    for c in counters:
        all_dates.extend(list(c.keys()))   
    dates = remove_duplicates(all_dates)
    dates.sort()
    
    # Sort all the data by dates
    candidates_count = []
    for c in counters:
        sorted_count = []
        for d in dates:
            if c.get(d) is None:
                sorted_count.append(0)
            else:
                sorted_count.append(c[d])
        candidates_count.extend([sorted_count])
        
    # Create the data frame
    d = {"Dates" : dates}
    
    for j in range(0,len(names)):
        d[names[j]] = candidates_count[j]
        
    data_frame = pd.DataFrame(data=d)
    return data_frame
    
# Reading of each file and relate the counting to the canditate
for in_name in Path("raw_data").rglob("*.txt"):
    if in_name.stem.split("-")[-1] in biden_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= biden_count)
    elif in_name.stem.split("-")[-1] in bloomberg_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= bloomberg_count)
    elif in_name.stem.split("-")[-1] in buttigieg_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= buttigieg_count)
    elif in_name.stem.split("-")[-1] in gabbard_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= gabbard_count)
    elif in_name.stem.split("-")[-1] in klobuchar_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= klobuchar_count)
    elif in_name.stem.split("-")[-1] in sanders_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= sanders_count)
    elif in_name.stem.split("-")[-1] in warren_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= warren_count)
    elif in_name.stem.split("-")[-1] in yang_queries:
        print(in_name)
        count_tweets(file_name=in_name, candidate_count= yang_count)

# Create the data frame and export it to a .csv file
all_counters = [biden_count, bloomberg_count, buttigieg_count,
            gabbard_count, klobuchar_count, sanders_count, 
            warren_count, yang_count] 

candidates_names = ["Joe Biden", "Mike Bloomberg", "Pete Buttigieg", 
                    "Tulsi Gabbard", "Amy Klobuchar", "Bernie Sanders",
                    "Elizabeth Warren", "Andrew Yang"]

df = sort_data(counters = all_counters, names = candidates_names)

df.to_csv('data/queries_count.csv',index = None)
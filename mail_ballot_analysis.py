# mail_ballot_analysis.py

import requests
import pandas as pd
import re
from datetime import datetime

# 1. pull data from the API, page through until we hit no more rows
url = "https://data.pa.gov/resource/mcba-yywm.json"
limit = 50000
offset = 0
all_records = []

print("Starting data pull…")
while True:
    resp = requests.get(url, params={"$limit": limit, "$offset": offset})
    resp.raise_for_status()  # blow up if something bad happens
    batch = resp.json()
    if not batch:
        break
    all_records.extend(batch)
    print(f"  got {len(batch)} records (total so far: {len(all_records)})")
    offset += limit

# 2. ingest into DataFrame
application_in = pd.DataFrame(all_records)

# 3. separate out rows with ANY null into invalid_data
invalid_data = application_in[application_in.isnull().any(axis=1)].copy()
application_in = application_in.dropna(how="any").copy()

# 4. convert senate district entries to snake_case
def to_snake(text):
    txt = text.strip().lower()
    txt = re.sub(r"[^a-z0-9]+", "_", txt)  # non-alphanum → underscore
    txt = re.sub(r"_+", "_", txt)           # squash repeats
    return txt.strip("_")

application_in["senate"] = application_in["senate"].apply(to_snake)

# 5. create yr_born field right after dateofbirth
application_in["dateofbirth"] = pd.to_datetime(application_in["dateofbirth"])
yr_index = application_in.columns.get_loc("dateofbirth") + 1
application_in.insert(yr_index,
                      "yr_born",
                      application_in["dateofbirth"].dt.year.astype(int))

# extra helpful field: age in years
current_year = datetime.now().year
application_in["age"] = current_year - application_in["yr_born"]

# parse application & return dates for latency
application_in["applicationdate"] = pd.to_datetime(application_in["applicationdate"])
application_in["dateballotreturned"] = pd.to_datetime(application_in["dateballotreturned"])
application_in["latency_days"] = (
    application_in["dateballotreturned"] - application_in["applicationdate"]
).dt.days

if __name__ == "__main__":
    # Q1: How does age & party relate to total requests?
    print("\n=== Requests by Party and Age ===")
    pivot = application_in.groupby(["party", "age"]).size().unstack(fill_value=0)
    print(pivot.head(10))  # show first 10 age cols
    
    # Q2: median latency per legislative district
    print("\n=== Median Latency (days) by Legislative District ===")
    med_lat = application_in.groupby("legislative")["latency_days"].median().sort_index()
    print(med_lat)
    
    # Q3: which congressional district requested ballots most?
    top_cd = application_in["congressional"].value_counts().idxmax()
    print(f"\nTop congressional district by # of requests: {top_cd}")

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time
import random

start = time.time()

url = "https://bash.im"
r = requests.get(url)

soup = BeautifulSoup(r.text, "html.parser")
endpoint = int(soup.find("input", "pager__input")["max"])

data = []
data_error = []
point = 1
endpoint = 100 # TODO comment this
while point <= endpoint:
    print(f"run page #{point}/{endpoint}")
    try:
        r = requests.get(f"https://bash.im/index/{point}")
    except requests.ConnectionError:
        sleep_time = random.randint(1, 5)
        print(f"can't get page #{point}. try again in {sleep_time} sec...")
        time.sleep(sleep_time)
        continue

    if r.status_code != 200:
        sleep_time = random.randint(1, 5)
        print(f"can't get page #{point}, status code {r.status_code}. try again in {sleep_time} sec...")
        time.sleep(sleep_time)
        continue

    soup = BeautifulSoup(r.text, "html.parser")

    candidates = soup.find_all("div", "quote__button share")
    page_data = []
    for quote in candidates:
        try:
            quote_json = json.loads(quote["data-share"])
            quote_json["id"] = int(quote_json["id"])
            data.append(quote_json)
        except json.JSONDecodeError:
            data_error.append(quote)
    point += 1

dataframe = pd.DataFrame(data)
dataframe.to_csv("bash-im.csv")

end = time.time()
print(f"finished with {round(end - start, 2)} sec.")

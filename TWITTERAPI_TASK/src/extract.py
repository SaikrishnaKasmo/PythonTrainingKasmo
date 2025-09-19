import json

def extraction(requests,url,headers,params): 
   # Request
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code}, {response.text}")
    raw_data = response.json()
    # Save raw JSON to file
    with open("tweets_raw.json", "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=4)
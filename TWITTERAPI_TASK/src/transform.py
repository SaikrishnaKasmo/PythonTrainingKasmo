import pandas as pd
import re

def clean_text(text):
    text = re.sub(r"http\S+","",text)
    text = re.sub(r"@\w+","",text)
    text = re.sub(r'#\w+',"",text)
    text = re.sub(r"[^A-Za-z0-9\s]+","",text)
    return text.strip()

def transformation(df):
    df = df.copy()
    df = pd.json_normalize(df)

    df = df.rename(columns={
        "id": "tweet_id",
    "public_metrics.retweet_count": "retweet_count",
    "public_metrics.like_count": "like_count",
    "public_metrics.reply_count": "reply_count",
    "public_metrics.quote_count": "quote_count"
    })
    df1 = df[["tweet_id","created_at","text","author_id","retweet_count","like_count"]]

    # Transformations
    df1["clean_text"] = df1["text"].apply(clean_text)
    df1["created_at"] = pd.to_datetime(df1["created_at"])
    df1 = df1.drop_duplicates(subset="tweet_id")

    return df1
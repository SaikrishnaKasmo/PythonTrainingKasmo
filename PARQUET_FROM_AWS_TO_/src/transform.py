import pandas as pd

def transform(house_df,weather_df):
    #Transformation for house_df
    house_df["mainroad"] = house_df["mainroad"].str.lower().map({"yes": True, "no": False})
    house_df["guestroom"] = house_df["guestroom"].str.lower().map({"yes": True, "no": False})
    house_df["basement"] = house_df["basement"].str.lower().map({"yes": True, "no": False})
    house_df["hotwaterheating"] = house_df["hotwaterheating"].str.lower().map({"yes": True, "no": False})
    house_df["airconditioning"] = house_df["airconditioning"].str.lower().map({"yes": True, "no": False})
    house_df["prefarea"] = house_df["prefarea"].str.lower().map({"yes": True, "no": False})

    ranges = {
    "price": (10000, 20000000),
    "area": (200, 10000),
    "bedrooms": (1, 6),
    "bathrooms": (1, 6),
    "stories": (1, 4),
    "parking": (0, 3)
    }

    house_df["outlier_flag"] = 0 
    for col, (min_val, max_val) in ranges.items():
        house_df["outlier_flag"] = house_df["outlier_flag"] | ((house_df[col] < min_val) | (house_df[col] > max_val))

    # Convert boolean to integer
    house_df["outlier_flag"] = house_df["outlier_flag"].astype(int)
    return house_df,weather_df

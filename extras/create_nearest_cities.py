import numpy as np
import pandas as pd
import json
import reverse_geocoder as rg

df = pd.read_csv('../data/airport-codes.csv')
new = df["coordinates"].str.split(",", n=1, expand=True)

df["latitude_deg"] = new[1]

df["longitude_deg"] = new[0]

df["latitude_deg"] = df["latitude_deg"].str.strip()
df["latitude_deg"] = df["latitude_deg"].astype(np.float)
df["longitude_deg"] = df["longitude_deg"].astype(np.float)

nearest_city = []
coord = list(zip(df["latitude_deg"].values.tolist(),df["longitude_deg"].values.tolist()))
result = rg.search(coord)
result = pd.DataFrame(json.loads(json.dumps(result)))
result.columns = ['lat','lon','nearest_city','nearest_city_state','nearest_city_county','country']
df.drop(["latitude_deg","longitude_deg"],axis=1,inplace=True)
df['nearest_city'] = result['nearest_city']
df.to_csv("./data/airport-codes_v2.csv",index=False)

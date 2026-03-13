import requests
import pandas as pd

URL = "https://data.cityofchicago.org/resource/4dn8-eb3h.json"


def fetch_inspections(limit: int = 1000) -> pd.DataFrame:
    response = requests.get(URL, params={"$limit": limit})
    response.raise_for_status()
    return pd.DataFrame(response.json())


if __name__ == "__main__":
    df = fetch_inspections()
    print(df.shape)
    print(df.columns.tolist())
    print(df.head())

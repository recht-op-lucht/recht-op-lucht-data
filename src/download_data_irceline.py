"""
Script to download temperature and NO2 data from IRCEL-CELINE.

Usage:

    # download all data from 2021 up to now
    python src/download_data_irceline.py

    # download all data from 2021 up to end of January 2022
    python src/download_data_irceline.py 2022-01-31

"""
import datetime
from dateutil import relativedelta
import json
import pathlib
import requests
import sys

import pandas as pd


DATA_DIR = pathlib.Path(".") / "data"


# -----------------------------------------------------------------------------
# Temperature stations

response = requests.get("https://geo.irceline.be/sos/api/v1/timeseries?phenomenon=62101")
stations_all = pd.json_normalize(json.loads(response.content))
stations_all

stations_antwerpen = stations_all[
    stations_all["station.properties.label"].isin([
        "42R802 - Borgerhout",
        "42R803 - Antwerpen (Park Spoor Noord)",
        "42R804 - Antwerpen (Ring)",
        "42R805 - Antwerpen (Belgiëlei)",
        "42R817 - Antwerpen (Groenenborgerlaan)",
        "42R818 - Antwerpen (Burchtse Weel)",
        "42M802 - Antwerpen"
    ])
]

STATIONS_TEMP = list(zip(stations_antwerpen["id"], stations_antwerpen["station.properties.label"]))


# -----------------------------------------------------------------------------
# NO2 stations

response = requests.get("https://geo.irceline.be/sos/api/v1/timeseries?phenomenon=8")
stations_all = pd.json_normalize(json.loads(response.content))
stations_all

stations_antwerpen = stations_all[
    stations_all["station.properties.label"].isin([
        "42R801 - Borgerhout",
        "42R802 - Borgerhout",
        "42R803 - Antwerpen (Park Spoor Noord)",
        "42R804 - Antwerpen (Ring)",
        "42R805 - Antwerpen (Belgiëlei)",
        "42R806 - Maerlant",
        "42R817 - Antwerpen (Groenenborgerlaan)",
    ])
]

STATIONS_NO2 = list(zip(stations_antwerpen["id"], stations_antwerpen["station.properties.label"]))


# -----------------------------------------------------------------------------
# Download functions

def download_month(stations, start):
    """
    Download one month of data.
    """
    end = start + relativedelta.relativedelta(months=1) - datetime.timedelta(hours=1)
    start = start.strftime("%Y-%m-%dT%H:%M:%S")
    end = end.strftime("%Y-%m-%dT%H:%M:%S")
    
    dfs = []

    for station_id, station_name in stations:
        response = requests.get(
            f"https://geo.irceline.be/sos/api/v1/timeseries/{station_id}/getData?timespan={start}/{end}"
        )
        df = pd.DataFrame(json.loads(response.content)["values"])
        # timestamp -> unix timestamp in UTC -> convert to local time
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert("Europe/Brussels")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["station"] = station_name
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)
    return data


def is_incomplete_last_file(filename, last_file, end):
    
    if filename == last_file:
        last_date = pd.read_csv(filename).iloc[-1, 0]
        if not last_date.startswith(end.strftime("%Y-%m-%d")):
            return True
    return False


def download(var, stations, start=datetime.datetime(2021, 1, 1), end=datetime.datetime.now()):
    """
    Download data from start of 2021 up to now.
    """
    data_dir_vmm = DATA_DIR / "external" / "vmm"
    data_dir_vmm.mkdir(exist_ok=True)
    
    # download month by month to avoid having to re-download all data
    # every time when updating
    existing_files = list(data_dir_vmm.glob(f"vmm-{var}-*.csv"))
    last_file = sorted(existing_files)[-1] if existing_files else ""
    
    date = start
    while date < end:
        filename = data_dir_vmm / f"vmm-{var}-{date.year}-{date.month}.csv"
        if filename.exists() and not is_incomplete_last_file(filename, last_file, end):
            print(f"Skipped {filename}")
            date += relativedelta.relativedelta(months=1)
            continue
        data = download_month(stations, date)
        data.to_csv(filename, index=False)
        print(f"Saved {filename}")
        date += relativedelta.relativedelta(months=1)

    # concatenate downloaded montly data into a single file
    dfs = []
    
    for filename in sorted(data_dir_vmm.glob(f"vmm-{var}-*.csv")):
        df = pd.read_csv(filename)
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)
    data = data.pivot(index="timestamp", columns="station", values="value")
    data.to_csv( DATA_DIR / "external" / f"vmm-{var}.csv")
    print(f'Created {DATA_DIR / "external" / f"vmm-{var}.csv"}')


if __name__ == "__main__":
    print(f"Downloading data to {DATA_DIR.resolve()}")

    args = sys.argv[1:]
    if len(args) == 0:
        download("no2", STATIONS_NO2)
        download("temperature", STATIONS_TEMP)
    elif len(args) == 1:
        end = datetime.datetime.strptime(args[0], "%Y-%m-%d")
        download("no2", STATIONS_NO2, end=end)
        download("temperature", STATIONS_TEMP, end=end)
    else:
        raise ValueError("unsupported number of arguments")

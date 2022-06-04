"""
Script to download atmospheric pressure data from Waterinfo
for the station of Melsele.

Usage:

    # download all data from 2021 up to now
    python src/download_data_waterinfo.py

    # download all data from 2021 up to end of January 2022
    python src/download_data_irceline.py 2022-01-31

"""
import datetime
from dateutil import relativedelta
import pathlib
import sys

import pandas as pd
from natsort import natsorted

import pywaterinfo


DATA_DIR = pathlib.Path(".") / "data"


vmm = pywaterinfo.Waterinfo("vmm")
# melsele = vmm.get_timeseries_list(station_name="Melsele_ME")
# melsele[melsele["stationparameter_longname"] == "Barometric pressure"][["stationparameter_longname", "ts_id", "ts_name", "ts_unitname", "ts_spacing"]]
# -> 78039042 is the timeseries ID for PT15M (15min timeseries)


def download_month(start):
    """
    Download one month of data.
    """
    end = start + relativedelta.relativedelta(months=1) - datetime.timedelta(minutes=15)
    end = min(end, datetime.datetime.now())
    start = start.strftime("%Y-%m-%dT%H:%M:%S")
    end = end.strftime("%Y-%m-%dT%H:%M:%S")

    data = vmm.get_timeseries_values(
        "78039042", start=start, end=end, returnfields="Timestamp,Value,Quality Code"
    )
    data = data.drop(columns=["ts_id"])
    return data


def is_incomplete_last_file(filename, last_file, end):
    
    if filename == last_file:
        last_date = pd.read_csv(filename).iloc[-1, 0]
        if not last_date.startswith(end.strftime("%Y-%m-%d")):
            return True
    return False


def download(start=datetime.datetime(2021, 1, 1), end=datetime.datetime.now()):
    """
    Download data from start of 2021 up to now.
    """
    data_dir_vmm = DATA_DIR / "external" / "vmm"
    data_dir_vmm.mkdir(exist_ok=True)
    
    # download month by month to avoid having to re-download all data
    # every time when updating
    existing_files = list(data_dir_vmm.glob(f"vmm-pressure-*.csv"))
    last_file = sorted(existing_files)[-1] if existing_files else ""
    
    date = start
    while date < end:
        filename = data_dir_vmm / f"vmm-pressure-{date.year}-{date.month}.csv"
        if filename.exists() and not is_incomplete_last_file(filename, last_file, end):
            print(f"Skipped {filename}")
            date += relativedelta.relativedelta(months=1)
            continue
        data = download_month(date)
        data.to_csv(filename, index=False)
        print(f"Saved {filename}")
        date += relativedelta.relativedelta(months=1)

    # concatenate downloaded montly data into a single file
    dfs = []
    
    for filename in natsorted(data_dir_vmm.glob(f"vmm-pressure-*.csv"), key=lambda y: str(y)):
        df = pd.read_csv(filename)
        dfs.append(df)

    data = pd.concat(dfs, ignore_index=True)
    data.to_csv( DATA_DIR / "external" / f"vmm-pressure.csv", index=False)
    print(f'Created {DATA_DIR / "external" / f"vmm-pressure.csv"}')


if __name__ == "__main__":
    print(f"Downloading data to {DATA_DIR.resolve()}")

    args = sys.argv[1:]
    if len(args) == 0:
        download()
    elif len(args) == 1:
        end = datetime.datetime.strptime(args[0], "%Y-%m-%d")
        download(end=end)
    else:
        raise ValueError("unsupported number of arguments")

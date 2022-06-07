"""
Calculate the equivalent NO2 concentration at the VMM reference stations
for the co-located tubes (for the calibration analysis).

python src/calculate_calibration_reference.py

Saves to data/rol_measurements_calibration.csv
"""
import pathlib

import pandas as pd


DATA_DIR = pathlib.Path(".") / "data"


data = pd.read_parquet(DATA_DIR / "rol_measurements.parquet")
data_vmm = pd.read_csv(DATA_DIR / "external" / "vmm-no2.csv")
# parse timestamp column and set as local time of the measurements
data_vmm["timestamp"] = pd.to_datetime(data_vmm["timestamp"], utc=True).dt.tz_convert("Europe/Brussels")
data_vmm = data_vmm.set_index("timestamp").sort_index()


dfs = []

for period in range(2, 14):
    for location, vmm_station in [
        ( "VMM Groenenborger", "42R817 - Antwerpen (Groenenborgerlaan)"),
        ("VMM Belgiëlei", "42R805 - Antwerpen (Belgiëlei)"),
        ("Van Maerlant", "42R806 - Maerlant")
    ]:
        subset = data[(data["period"] == period) & (data["location"] == location)].copy()
        if len(subset) == 0:
            assert period < 8 and location == "Van Maerlant"
            continue
        # Extract VMM reference measurements for the same time period
        start = subset["start_date"].iloc[0]
        end = subset["end_date"].iloc[0]
        vmm_measurements = data_vmm.loc[start:end, vmm_station]
        # Add average reference concentration and percentage of missing data
        subset["no2_reference"] = vmm_measurements.mean()
        subset["missing"] = vmm_measurements.isna().mean() * 100
        dfs.append(subset)


data_calibration = pd.concat(dfs, ignore_index=True)

data_calibration.to_csv(DATA_DIR / "rol_measurements_calibration.csv", index=False)

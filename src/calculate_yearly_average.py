"""
Calculate estimated yearly average NO2 concentration for each location (
including a calibration factor (see notebooks/calibration.ipynb) and
estimate of period 1 / january (see notebooks/inschatting-januari.ipynb)).

python src/calculate_yearly_average.py

Saves to data/rol_measurements_yearly.csv
"""
import pathlib

import pandas as pd


DATA_DIR = pathlib.Path(".") / "data"


df = pd.read_parquet(DATA_DIR / "rol_measurements.parquet")


# Add an estimate of period 1 (January), see notebooks/inschatting-januari.ipynb
df_january = df[df["period"] == 2].copy()
df_january["no2"]  = df_january["no2"] / 0.98
df_january["period"] = 1
df_january["end_date"] = df_january["start_date"]
df_january["start_date"] = pd.Timestamp("2021-01-01 12:00", tz="Europe/Brussels")
df = pd.concat([df_january, df], ignore_index=True)


def calculate_yearly_average(df):
    df = df.copy()
    # last period runs into 2022 -> cut off to adjust weight for this period
    df.loc[df["period"] == 13, "end_date"] = pd.Timestamp("2022-01-01 12:00:00+01:00", tz="Europe/Brussels")

    # weighted average taking into account the duration of the period of each measurement
    df["weight"] = (df["end_date"] - df["start_date"]).dt.total_seconds()
    weighted_average = (df["no2"] * df["weight"]).sum() / df["weight"].sum()

    # calibration factor
    return weighted_average * 1.051


# calculate yearly average per location
result = df.groupby("location").apply(calculate_yearly_average).rename("no2")


# save rounded and reorderd data
location_order = [
    "Deurnsebaan Zuid", "Deurnsebaan Noord",
    "Van de Perre Zuid", "Van de Perre Noord",
    "Turnhoutsebaan BO West", "Turnhoutsebaan BO Oost",
    "Turnhoutsebaan DE West", "Turnhoutsebaan DE Oost",
    "Provinciestraat Zuid", "Provinciestraat Noord",
    "Pothoekstraat Zuid","Pothoekstraat Noord",
    "Borsbeekstraat Zuid", "Borsbeekstraat Noord",
    "Klappeistraat", "Gijselstraat",
    "Sint-Bernardsesteenweg", "Abdijstraat",
    "Nationalestraat Zuid", "Nationalestraat Noord",
    "Cassiersstraat West", "Cassiersstraat Oost",
    "VMM Groenenborger", "VMM Belgiëlei",
    "Van Maerlant", "Kennedy_mid", "Kennedy_lift"]

result = result.reindex(location_order).round(1).reset_index()
result.to_csv(DATA_DIR / "rol_measurements_yearly.csv", index=False)

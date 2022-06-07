"""
Process raw measurements of the different periods (files from Buro Blauw laboratory)
and combine in a single file.

python src/process_raw_measurements.py

Saves to data/rol_measurements.csv
"""
import pathlib

import pandas as pd


DATA_DIR = pathlib.Path(".") / "data"


def read_buro_blauw_results(path, periode):
    df = pd.read_excel(path, skiprows=[0, 1, 2, 4])
    # clean-up column names
    df.columns = df.columns.str.strip()
    # combine + localize start / end timestamps
    df["Startdatum"] = df.Startdatum + pd.to_timedelta(df.Starttijd.map(lambda x: x.isoformat()))
    df["Einddatum"] = df.Einddatum + pd.to_timedelta(df.Eindtijd.map(lambda x: x.isoformat()))
    df["Startdatum"] = df["Startdatum"].dt.tz_localize("Europe/Brussels")
    df["Einddatum"] = df["Einddatum"].dt.tz_localize("Europe/Brussels")
    df = df.drop(columns=["Starttijd", "Eindtijd"])
    # drop Field Blanco locations
    df = df[~df["Blauw"].str.contains("VB")]
    df["Concentratie NO2"] = df["Concentratie NO2"].replace({"<15": "15"}).astype("int")
    df["Periode"] = periode
    df = df.loc[:, ~df.columns.str.contains("Unnamed")]
    return df.copy()


dfs = []

for period in range(2, 14):
    filename = f"Rapportage_2021_{period:02d}.xlsx"
    df = read_buro_blauw_results(DATA_DIR / "raw" / filename, periode=period)
    dfs.append(df)

data = pd.concat(dfs)


# Remove incorrect measurements for Deurnsebaan
# (only measured for two days, concentrations not reliable)
data = data[~data["Blauw"].isin(["142-1", "142-2", "142-3", "142-4"])].copy()


# Clean-up descriptions
correcties = {
    "Abdijstraat": "Abdijstraat",
    "Borsbeekstraat 1130": "Borsbeekstraat Zuid",
    "Borsbeekstraat 1135": "Borsbeekstraat Noord",
    "Borsbeekstraat Noord": "Borsbeekstraat Noord",
    "Borsbeekstraat zuid": "Borsbeekstraat Zuid",
    "Cassiers 11587": "Cassiersstraat West",
    "Cassiers 11590": "Cassiersstraat Oost",
    "Cassiersstraat Oost": "Cassiersstraat Oost",
    "Cassiersstraat Oost ": "Cassiersstraat Oost",
    "Cassiersstraat West": "Cassiersstraat West",
    "Deurnsebaan Noord": "Deurnsebaan Noord",
    "Deurnsebaan Zuid": "Deurnsebaan Zuid",
    "Deurnsebaan-329": "Deurnsebaan Zuid",
    "Deurnsebaan-330": "Deurnsebaan Noord",
    "Gijselstraat ": "Gijselstraat",
    "Klappeistraat": "Klappeistraat",
    "Nationale Aveve": "Nationalestraat Zuid",
    "Nationale T2": "Nationalestraat Noord",
    "Nationalestraat Noord": "Nationalestraat Noord",
    "Nationalestraat Zuid": "Nationalestraat Zuid",
    "Pothoekstraat 1714": "Pothoekstraat Zuid",
    "Pothoekstraat noord": "Pothoekstraat Noord",
    "Pothoekstraat zuid": "Pothoekstraat Zuid",
    "Pothoekstraat zwart": "Pothoekstraat Noord",
    "Provinciestraat Zuid": "Provinciestraat Zuid",
    "Provinciestraat noord": "Provinciestraat Noord",
    "Sint Bernardse Steenweg": "Sint-Bernardsesteenweg",
    "Sint-Bernardsesteenweg": "Sint-Bernardsesteenweg",
    "THB-1307": "Turnhoutsebaan BO West",
    "THB-1328": "Turnhoutsebaan BO Oost",
    "TurnBaan DE oost": "Turnhoutsebaan DE Oost",
    "TurnBaan DE oost 25081": "Turnhoutsebaan DE Oost",
    "TurnBaan DE west 25081": "Turnhoutsebaan DE Oost",
    "TurnBaan DE west": "Turnhoutsebaan DE West",
    "TurnBaan DE west 25071": "Turnhoutsebaan DE West",
    "Turnhoutsebaan oost": "Turnhoutsebaan BO Oost",
    "Turnhoutsebaan west": "Turnhoutsebaan BO West",
    "VMM Belgiëlei": "VMM Belgiëlei",
    "VMM Groenenborger": "VMM Groenenborger",
    "Van de Perre 2151": "Van de Perre Zuid",
    "Van de Perre 2158": "Van de Perre Noord",
    "Van de Perre Noord": "Van de Perre Noord",
    "Van de Perre Zuid": "Van de Perre Zuid",
    "nationalestraat zuid": "Nationalestraat Zuid",
}
data["Omschrijving"] = data["Omschrijving"].replace(correcties)


# For some periods, tubes for two locations in a street had been put in the
# wrong location
assert data.loc[(data["Periode"] == 7) & (data["Omschrijving"] == "Cassiersstraat West") & (data["Blauw"] == "144-45"), "Concentratie NO2"].item() == 27
assert data.loc[(data["Periode"] == 7) & (data["Omschrijving"] == "Cassiersstraat Oost") & (data["Blauw"] == "144-44"), "Concentratie NO2"].item() == 33
data.loc[(data["Periode"] == 7) & (data["Blauw"] == "144-44"), "Omschrijving"] = "Cassiersstraat West"
data.loc[(data["Periode"] == 7) & (data["Blauw"] == "144-45"), "Omschrijving"] = "Cassiersstraat Oost"

assert data.loc[(data["Periode"] == 12) & (data["Omschrijving"] == "Cassiersstraat West") & (data["Blauw"] == "366-43"), "Concentratie NO2"].item() == 40
assert data.loc[(data["Periode"] == 12) & (data["Omschrijving"] == "Cassiersstraat Oost") & (data["Blauw"] == "366-45"), "Concentratie NO2"].item() == 38
data.loc[(data["Periode"] == 12) & (data["Blauw"] == "366-43"), "Omschrijving"] = "Cassiersstraat Oost"
data.loc[(data["Periode"] == 12) & (data["Blauw"] == "366-45"), "Omschrijving"] = "Cassiersstraat West"

assert (data.loc[(data["Periode"] == 3) & (data["Blauw"].isin(["35-17", "35-18"])), "Omschrijving"] == "Provinciestraat Zuid").all()
assert (data.loc[(data["Periode"] == 3) & (data["Blauw"].isin(["35-21", "35-22"])), "Omschrijving"] == "Provinciestraat Noord").all()
data.loc[(data["Periode"] == 3) & (data["Blauw"].isin(["35-17", "35-18"])), "Omschrijving"] = "Provinciestraat Noord"
data.loc[(data["Periode"] == 3) & (data["Blauw"].isin(["35-21", "35-22"])), "Omschrijving"] = "Provinciestraat Zuid"


# Rename columns

data = data.rename(columns={
    "Blauw": "code",
    "Omschrijving": "location",
    "Startdatum": "start_date",
    "Einddatum": "end_date",
    "Concentratie NO2": "no2",
    "Periode": "period",
})

data.to_parquet(DATA_DIR / "rol_measurements.parquet", index=False)
data.to_csv(DATA_DIR / "rol_measurements.csv", index=False)

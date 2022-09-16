from datetime import datetime
import pandas as pd
import os
from tqdm import tqdm
import sqlite3


def write_log(msg: str):
    """
    Creates/updates error logs in log.txt file within the parent directory
    Args:
        msg (str): Error message which needs to be appended in the log.txt file.
    """
    with open("log.txt", "a") as f:
        f.writelines(msg)


def create_weather_df() -> pd.DataFrame:
    """
    Scans the wx_data direcotry for all weather data available in StationID.txt
    files and collates all data into a dataframe
    Returns:
    pd.DataFrame: weather dataframe
    """
    Date = []
    MaxTemp = []
    MinTemp = []
    Precipitation = []
    StationID = []

    for weather_file in tqdm(os.listdir("wx_data")):
        try:
            f_path = os.path.abspath(f"wx_data/{weather_file}")
            station_name = f_path.split("/")[-1].split(".")[0]
            df = pd.read_csv(f_path, delimiter="\t", header=None)
            df.columns = ["Date", "MaxTemp", "MinTemp", "Precipitation"]
            df["Date"] = df["Date"].apply(
                lambda x: (datetime.strptime(str(x), "%Y%m%d").strftime("%d-%b-%Y"))
            )
            df["MaxTemp"] = df["MaxTemp"] / 10
            df["MinTemp"] = df["MinTemp"] / 10
            df["Precipitation"] = df["Precipitation"] / 10
            df["StationID"] = station_name
            Date += list(df["Date"])
            MaxTemp += list(df["MaxTemp"])
            MinTemp += list(df["MinTemp"])
            Precipitation += list(df["Precipitation"])
            StationID += list(df["StationID"])
        except:
            write_log(f"Invalid format detected for file: {f_path}")
            continue

    res_df = pd.DataFrame(
        data={
            "ID": [ind + 1 for ind in range(len(Date))],
            "Date": Date,
            "MaxTemp": MaxTemp,
            "MinTemp": MinTemp,
            "Precipitation": Precipitation,
            "StationID": StationID,
        }
    )
    return res_df


def create_weather_table(db: str, weather_df: pd.DataFrame):
    """
    Adds weather table into source db using Weather dataframe information containing relavent
    temperature, precipitation and station ID information
    Args:
        db (str): source database path
        weather_df (pd.DataFrame): weather dataframe
    """
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS weather (ID number, Date text, MaxTemp real, MinTemp
    real, Precipitation real, StationID text)"""
    )
    conn.commit()
    weather_df.to_sql("weather", conn, if_exists="replace", index=False)
    conn.close()


def create_yield_df() -> pd.DataFrame:
    """
    Scans the yld_data direcotry for all weather data available in all yield files and collates all data into a dataframe
    Returns:
        pd.DataFrame: Yield dataframe
    """
    Year = []
    TotalYield = []

    print("Capturing Yield data...")
    for yield_file in tqdm(os.listdir("yld_data")):
        try:
            f_path = os.path.abspath(f"yld_data/{yield_file}")
            df = pd.read_csv(f_path, delimiter="\t", header=None)
            df.columns = ["Year", "TotalYield"]

            Year += list(df["Year"])
            TotalYield += list(df["TotalYield"])
        except:
            write_log(f"Invalid format detected for file: {f_path}")
            continue
    res_df = pd.DataFrame(
        data={
            "ID": [ind + 1 for ind in range(len(Year))],
            "Year": Year,
            "TotalYield": TotalYield,
        }
    )
    return res_df


def create_yield_table(db: str, yield_df: pd.DataFrame):
    """
    Adds yield table into source db using yield dataframe information containing relavent temperature, precipitation and station ID information

    Args:
        db (str): source database path
        yield_df (pd.DataFrame): yield dataframe
    """
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS yield (ID number, Year text, TotalYield number)"
    )
    conn.commit()
    yield_df.to_sql("yield", conn, if_exists="replace", index=False)
    conn.close()


def create_resultant_df(weather_df: pd.DataFrame) -> pd.DataFrame:
    """

    Returns:
        pd.DataFrame: Yield dataframe
    """
    station_ids = list(weather_df["StationID"].unique())
    years = list(weather_df["Date"].apply(lambda x: x.split("-")[-1]).unique())

    res_year = []
    res_station_id = []
    avg_maxtemp = []
    avg_mintemp = []
    avg_precipitation = []

    for year in years:
        print("Calculating means for all stations in the Year ", year)
        for station_id in tqdm(station_ids):
            res_year.append(year)
            res_station_id.append(station_id)
            avg_maxtemp.append(
                weather_df[
                    (weather_df["Date"].apply(lambda x: x.split("-")[-1]) == year)
                    & (weather_df["StationID"] == station_id)
                    & (weather_df["MaxTemp"] != -999.9)
                    & (weather_df["MinTemp"] != -999.9)
                    & (weather_df["Precipitation"] != -999.9)
                ].MaxTemp.mean()
            )
            avg_mintemp.append(
                weather_df[
                    (weather_df["Date"].apply(lambda x: x.split("-")[-1]) == year)
                    & (weather_df["StationID"] == station_id)
                    & (weather_df["MaxTemp"] != -999.9)
                    & (weather_df["MinTemp"] != -999.9)
                    & (weather_df["Precipitation"] != -999.9)
                ].MinTemp.mean()
            )
            avg_precipitation.append(
                weather_df[
                    (weather_df["Date"].apply(lambda x: x.split("-")[-1]) == year)
                    & (weather_df["StationID"] == station_id)
                    & (weather_df["MaxTemp"] != -999.9)
                    & (weather_df["MinTemp"] != -999.9)
                    & (weather_df["Precipitation"] != -999.9)
                ].Precipitation.mean()
            )

    res_df = pd.DataFrame(
        data={
            "ID": [ind + 1 for ind in range(len(res_year))],
            "Year": res_year,
            "StationID": res_station_id,
            "AverageMaxTemp": avg_maxtemp,
            "AverageMinTemp": avg_mintemp,
            "AveragePrecipitation": avg_precipitation,
        }
    )
    return res_df


def create_resultant_table(db: str, res_df: pd.DataFrame):
    """
    Adds result table into source db using result dataframe information containing relavent average temperatures, avg precipitation and station IDs for each year

    Args:
        db (str): source database path
        res_df (pd.DataFrame): resultant dataframe
    """
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute(
        "CREATE TABLE IF NOT EXISTS result (ID number, Year text, StationID text, AverageMaxTemp real, AverageMinTemp real, AveragePrecipitation real)"
    )
    conn.commit()
    res_df.to_sql("result", conn, if_exists="replace", index=False)
    conn.close()


db = "weather_yield.db"
weather_df = create_weather_df()
create_weather_table(db, weather_df)

yield_df = create_yield_df()
create_yield_table(db, yield_df)

resultant_df = create_resultant_df(weather_df)
create_resultant_table(db, resultant_df)

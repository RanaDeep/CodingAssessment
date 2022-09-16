# Summary
- Python automation using pandas to ingest data into SQLite database
- Ability to analyze data and create new data model from Data Analysis
- Used flask for implementation
- Api's to show all models data with some added filters
- Used uitls.py file for ingestion of data into database(sqlite)
- default parameters for pages


## Data Modeling
**Weather Data** :
Each StationID text file (under wx_data) contains its respective annual precipitation, maximum and minimum temperatures. All data entries are tab separated. Hence, `pandas` is used for quick parsing. Using Pandas, the data is converted into data frames and a column is added to indicate which StationID the data corresponds to. ID column added to use it as a Primary Key. The dataframe conversion is handled using the function `create_weather_df()` defined in `utils.py`.
**Yield Data** :
Similar to weather data parsing, the yield data is also parsed using pandas by converting the data into a dataframe which is handled using the function `create_yield_df()` defined in `utils.py`.

## Ingestion
Using `sqlite3` and `pandas` modules the weather and yield data frames are converted to respective weather and yield tables in the newly created weather_yield.db file. This is achieved using `create_weather_table()` and `create_yield_table()` functions defined in `utils.py`.

## Data Analysis
Since the weather data is already made available in the form of a dataframe, `Pandas` is used to perform data analysis.
For every year and every Station ID, the average annual precipitation, annual average maximum and minimum temperatures are computed and stored in a new resultant data frame.
All entries containing -9999 are ignored for any calculations.
Upon storing the resultant data in the form of a dataframe, `pandas` and `sqlite3` modules are used again for creating the `result` table in `weather_yield.db` which holds the computed weather data. All the data are stored with respective Year, StationID columns.

*Note*: Usage of `utils.py` helps generate the final `weather_yield.db` file which is then imported in the Flask API.
#### Usage: ```python utils.py```

## API
Implemented 3 endpoints that return JSON
- `/api/weather`
    - Query parameters
        - date, station_id, id
    - All arguments are optional, data will be filtered appropriately when date and/or station_id are provided
    - If both date & station_id are absent, all the records will be returned with pagination
- `/api/weather/stats`
    - Query parameters
        - year, station_id
- `/api/yield`
    - Query parameters
        - year


# Setup to run
- Developed and tested the application on `Win 11` with `Python 3.8.12`
```
py -m venv env

.\env\Scripts\activate

pip install -r requirements.txt
```

## File layout
- **utils.py** : Contains modules to create weather_yield.db databases with 3 tables: 
*Weather:* Table containing daily weather data for each StationID (precipitation, maximum and minimum temperatures)
*Yield:* Table containing year and the respective annual crop yield.
*Result:* Table containing the weather stats like the annual average rainfall, average maximum and minimum temperatures ignoring the entries containing -9999.
 The modules available under utils.py:
 write_log(msg:str):
   Creates/updates error logs in log.txt file within the parent directory
   Args:
       msg (str): Error message which needs to be appended in the log.txt file.
* create_weather_df() -> pd.DataFrame:
   Scans the wx_data directory for all weather data available in StationID.txt files and collates all data into a dataframe
 
   Returns:
       pd.DataFrame: weather dataframe
* create_weather_table(db:str, weather_df:pd.DataFrame):
   Adds weather table into source db using Weather data frame information containing relevant temperature, precipitation and station ID information
 
   Args:
       db (str): source database path
       weather_df (pd.DataFrame): weather dataframe
* create_yield_df() -> pd.DataFrame:
   Scans the yld_data direcotry for all weather data available in all yield files and collates all data into a dataframe
 
   Returns:
       pd.DataFrame: Yield dataframe
* create_yield_table(db:str, yield_df:pd.DataFrame):
   Adds yield table into source db using yield dataframe information containing relevant temperature, precipitation and station ID information
 
   Args:
       db (str): source database path
       yield_df (pd.DataFrame): yield dataframe
* create_resultant_df(weather_df:pd.DataFrame) -> pd.DataFrame
Computes the average weather stats for each station ID for each year. 
   Returns:
       pd.DataFrame: Yield dataframe
* create_resultant_table(db:str, res_df:pd.DataFrame):
   Adds result table into source db using result dataframe information containing relevant average temperatures, avg precipitation and station IDs for each year
 
   Args:
       db (str): source database path
       res_df (pd.DataFrame): resultant dataframe


- **app.py** : 
db_connect() : Using this to connect with the database 
index() : Display all pages url for the api’s
weather() : Display all weather data from the db
weather_id(id) : Display the weather data for specific id
weather_stationid(id) : Display the weather data for specific station id
yield_data() : Display all the yield data
yield_data_id(id) : Display the yield data for specific id
yield_data_year(year) : Display the yield data for specific year
result_data() : Display all the result data
result_data_year(year) : Display all the result data for specific year
result_data_station_id(station_id) : Display all the result data for specific station id


- Routes: URLs for app.py:
    - `/api/weather` : This may take a some time to load as we our loading all weather data consisting of 18,778,45 records. We can also use generators for minimizing the loading time. We can also use pagination for this.
    - `/api/weather/stats`
    - `/api/yield`


## Running web service
```
(env) PS code_task> python utils.py
 * Adding data in database 
```

# Run app.py
After this you can use the API's
```
(env) PS code_task> python app.py
 * Serving Flask app 'app'
 * Debug mode: on
WARNING: This is a development server. Do not use it in a production deployment. Use a production WSGI server instead.
 * Running on http://127.0.0.1:5000
Press CTRL+C to quit
 * Restarting with stat
 * Debugger is active!
 * Debugger PIN: 319-060-339

```

Screenshots of api's in browser can be found in `screenshots` folder.

## Sample linter run

```
(env) PS code_task> pylint app.py

-------------------------------------------------------------------
Your code has been rated at 10.00/10 (previous run: 9.58/10, +0.42)

(env) PS code_task> pylint utils.py

-------------------------------------------------------------------
Your code has been rated at 10.00/10 (previous run: 9.18/10, +0.82)

```

## Sample code formatter run

```
(env) PS code_task> black .\code_task\
reformatted code_task\utils.py
reformatted code_task\app.py

All done! ✨ 🍰 ✨
2 files reformatted.

```


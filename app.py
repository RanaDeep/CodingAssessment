from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)


def db_connect():
    """
    Connecting to the database using sqlite3
    """
    create_wx_db = None
    try:
        create_wx_db = sqlite3.connect("weather_yield.db")
    except sqlite3.Error as er:
        print(er)
    return create_wx_db


@app.route("/")
def index():
    """
    all api routes
    """
    results = {
        "result": {
            "weather": "http://127.0.0.1:5000/api/weather",
            "yield": "http://127.0.0.1:5000/api/yield",
            "weather_stats": "http://127.0.0.1:5000/api/weather_stats",
        }
    }
    return jsonify(results)


@app.route("/api/weather", methods=["GET"])
def weather():
    """
    To display all weather data
    """
    get_db = db_connect()
    cur = get_db.cursor()
    conn = cur.execute("select * from `weather`")
    weather_data = [
        dict(
            id=results[0],
            Date=results[1],
            max_temp=results[2],
            mini_temp=results[3],
            amount_pre=results[4],
            station_id=results[5],
        )
        for results in conn.fetchall()
    ]

    get_db.close()
    results = {
        "count": len(weather_data),
        "result": weather_data,
    }

    if weather_data is not None:
        return jsonify(results)


@app.route("/api/weather/id=<id>", methods=["GET"])
def weather_id(id):
    """
    To display all weather data for specific id
    """
    get_db = db_connect()
    cur = get_db.cursor()
    conn = cur.execute(f"select * from `weather` where id = {id}")
    weather_data = [
        dict(
            id=results[0],
            Date=results[1],
            max_temp=results[2],
            mini_temp=results[3],
            amount_pre=results[4],
            station_id=results[5],
        )
        for results in conn.fetchall()
    ]

    get_db.close()

    results = {
        "count": len(weather_data),
        "result": weather_data,
    }
    if weather_data is not None:
        return jsonify(results)


@app.route("/api/weather/stationid=<id>", methods=["GET"])
def weather_stationid(id):
    """
    To display all weather data for specific station id
    """
    get_db = db_connect()
    cur = get_db.cursor()
    conn = cur.execute(f"select * from weather where stationid = {id}")
    weather_data = [
        dict(
            id=results[0],
            Date=results[1],
            max_temp=results[2],
            mini_temp=results[3],
            amount_pre=results[4],
            station_id=results[5],
        )
        for results in conn.fetchall()
    ]
    get_db.close()

    results = {
        "count": len(weather_data),
        "result": weather_data,
    }

    if weather_data is not None:
        return jsonify(results)


@app.route("/api/yield", methods=["GET"])
def yield_data():
    """
    To display all yield data
    """
    get_yld_db = db_connect()
    cur = get_yld_db.execute(f"Select * from yield")

    yld_data = [
        dict(id=row[0], year=row[1], Mega_Tons=row[2]) 
        for row in cur.fetchall()
    ]

    results = {
        "count": len(yld_data),
        "result": yld_data,
    }

    if yld_data is not None:
        return jsonify(results)


@app.route("/api/yield/id=<id>", methods=["GET"])
def yield_data_id(id):
    """
    To display all yield data for specific id
    """
    get_yld_db = db_connect()

    cur = get_yld_db.execute(f"Select * from yield where id={id}")

    yld_data = [
        dict(id=row[0], year=row[1], Mega_Tons=row[2]) for row in cur.fetchall()
    ]

    results = {
        "count": len(yld_data),
        "result": yld_data,
    }

    if yld_data is not None:
        return jsonify(results)


@app.route("/api/yield/year=<year>", methods=["GET"])
def yield_data_year(year):
    """
    To display all yield data for specific year
    """
    get_yld_db = db_connect()

    cur = get_yld_db.execute(f"Select * from yield where year={year}")

    yld_data = [
        dict(id=row[0], year=row[1], Mega_Tons=row[2]) for row in cur.fetchall()
    ]

    results = {
        "count": len(yld_data),
        "result": yld_data,
    }

    if yld_data is not None:
        return jsonify(results)


@app.route("/api/weather_stats", methods=["GET"])
def result_data():
    """
    To display all result data
    """
    get_res_db = db_connect()
    cur = get_res_db.execute(f"Select * from result")

    res_data = [
        dict(
            id=row[0],
            year=row[1],
            station_id=row[2],
            avg_max_temp=row[3],
            avg_min_temp=row[4],
            avg_precipitation=row[5],
        )
        for row in cur.fetchall()
    ]

    results = {
        "count": len(res_data),
        "result": res_data,
    }

    if res_data is not None:
        return jsonify(results)


@app.route("/api/weather_stats/year=<year>", methods=["GET"])
def result_data_year(year):
    """
    To display all result data for specific year
    """
    get_res_db = db_connect()

    cur = get_res_db.execute(f"Select * from result where year={year}")

    res_data = [
        dict(
            id=row[0],
            year=row[1],
            station_id=row[2],
            avg_max_temp=row[3],
            avg_min_temp=row[4],
            avg_precipitation=row[5],
        )
        for row in cur.fetchall()
    ]

    results = {
        "count": len(res_data),
        "result": res_data,
    }

    if res_data is not None:
        return jsonify(results)


@app.route("/api/weather_stats/station_id=<station_id>", methods=["GET"])
def result_data_station_id(station_id):
    """
    To display all result data for station id
    """
    get_res_db = db_connect()

    cur = get_res_db.execute(f"Select * from result where StationID={station_id}")

    res_data = [
        dict(
            id=row[0],
            year=row[1],
            station_id=row[2],
            avg_max_temp=row[3],
            avg_min_temp=row[4],
            avg_precipitation=row[5],
        )
        for row in cur.fetchall()
    ]

    results = {
        "count": len(res_data),
        "result": res_data,
    }

    if res_data is not None:
        return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True)

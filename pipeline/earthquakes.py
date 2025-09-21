import requests
from datetime import datetime, timedelta, UTC
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
from pyspark.sql.types import * #imports all datatypes... to use for schema creation
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()


user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
host = os.getenv("MYSQL_HOST")  # Default to localhost if not set
port = os.getenv("MYSQL_PORT")  # Default to 3306 if not set
database = os.getenv("MYSQL_DB")  # Default to 'earthquake_db' if not set

JDBC_URL = f"jdbc:mysql://{host}:{port}/{database}"

def get_spark_session():
    return (
        SparkSession.builder
        .appName("Earthquake Data Processing")
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.32")
        .getOrCreate()
    )

# create table.. mysql requires primary key
def create_db_and_table():

    # create database safely
    MYSQL_URI_NO_DB = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/"
    engine = create_engine(MYSQL_URI_NO_DB)
    engine_no_db = create_engine(MYSQL_URI_NO_DB, pool_pre_ping=True)

    with engine_no_db.connect() as conn:
        print(f"Creating database if not exists: {database}")
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database}`;"))

    # reset engine state
    engine_no_db.dispose()


    # connect to db and create table
    MYSQL_URI = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"

    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS earthquake_events (
                id VARCHAR(100) PRIMARY KEY,
                place TEXT,
                magnitude DOUBLE,
                time DATETIME,
                longitude DOUBLE,
                latitude DOUBLE,
                depth DOUBLE
            );
        """))

        engine.dispose()
        print("Table 'earthquake_events' updated in MySQL.")

# fetch earthquake data from the USGS Earthquake API for the past 1 hr

def fetch_earthquake_data():
    try: 
        endtime = datetime.now(UTC)
        starttime = endtime - timedelta(minutes=60)

        # Format timestamps in ISO8601 as required by API
        start_str = starttime.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = endtime.strftime("%Y-%m-%dT%H:%M:%S")

        url = (
            "https://earthquake.usgs.gov/fdsnws/event/1/query"
            f"?format=geojson&starttime={start_str}&endtime={end_str}"
        )
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        features = data.get("features", [])
        earthquakes = []

        for f in features:
            props = f["properties"]
            coords = f["geometry"]["coordinates"]
            earthquakes.append({
                "id": f["id"],
                "place": props["place"],
                "magnitude": float(props["mag"]) if props["mag"] is not None else None,
                "time": datetime.fromtimestamp(props["time"] / 1000.0).isoformat(sep=' '),
                "longitude": float(coords[0]),
                "latitude": float(coords[1]),
                "depth": float(coords[2])
            })

        print(f"✅ fetch_earthquake_data: {len(earthquakes)} events fetched.")
        return earthquakes

    except Exception as e:
        print(f"Error fetching earthquake data: {e}")
        return []

    

# Convert to Spark DataFrame
def earthquakes_to_df(data):
    schema = StructType([
        StructField("id", StringType(), False),     #false = not nullable
        StructField("place", StringType(), True),
        StructField("magnitude", DoubleType(), True),
        StructField("time", TimestampType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth", DoubleType(), True)
    ])

    spark = get_spark_session()
    return spark.createDataFrame(data, schema=schema)

# write to mysql with spark
def load_to_mysql(df):
    if df.count() == 0:
        print("No recent earthquakes")
        return
    
    MYSQL_URI = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)

    upsert_query = text("""
        INSERT INTO earthquake_events (id, place, magnitude, time, longitude, latitude, depth)
        VALUES (:id, :place, :magnitude, :time, :longitude, :latitude, :depth)
        ON DUPLICATE KEY UPDATE
            place=VALUES(place),
            magnitude=VALUES(magnitude),
            time=VALUES(time),
            longitude=VALUES(longitude),
            latitude=VALUES(latitude),
            depth=VALUES(depth)
    """)

    with engine.begin() as conn:
        for row in df.collect():
            conn.execute(upsert_query, {
                "id": row["id"],
                "place": row["place"],
                "magnitude": row["magnitude"],
                "time": row["time"],
                "longitude": row["longitude"],
                "latitude": row["latitude"],
                "depth": row["depth"]
            })

    engine.dispose()
    print("Earthquake data upserted into MySQL.")


    """ # alternative to write to MySQL using Spark JDBC 
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{host}:{port}/{database}") \
        .option("dbtable", "earthquake_events") \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    print("Earthquake data loaded into MySQL.")

"""

"""
    alternative without schema... not recommended, but useful for quick testing:

    earthquakes = fetch_earthquake_data()
    df2 = spark.createDataFrame(earthquakes)  # No schema passed, datatypes are inferred automatically
    df2.show(truncate=False)
    df2.printSchema()
"""



# MYSQL_URI = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"

# # Load into MySQL using Pandas
# def load_to_mysql(df):
#     if df.count() == 0:
#         print("No new earthquake data.")
#         return

#     from pyspark.sql.functions import col

#     # Cast all timestamp columns to string before converting to Pandas to avoid type errors
#     df = df.select([
#         col(c).cast("string") if dtype == "timestamp" else col(c)
#         for c, dtype in df.dtypes
#     ])
#     # Convert Spark DataFrame to Pandas DataFrame for SQLAlchemy
#     pdf = df.toPandas()
#     engine = create_engine(MYSQL_URI)
#     pdf.to_sql('earthquake_events', con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    create_db_and_table()

    data = fetch_earthquake_data()

    if not data:
        print("No recent earthquakes.")
        exit()
    else:
        df = earthquakes_to_df(data)
        df.show()

        load_to_mysql(df)
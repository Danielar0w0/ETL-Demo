import pandas as pd
import json
import psycopg2
import sqlite3
import time


def wait_for_db():
    """Wait for the PostgreSQL service to be ready."""
    retries = 5
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname="etl_database",
                user="user",
                password="password",
                host="db",
                port=5432
            )
            conn.close()
            print("PostgreSQL is ready!")
            return
        except psycopg2.OperationalError:
            retries -= 1
            print("Waiting for PostgreSQL to be ready...")
            time.sleep(5)
    raise Exception("PostgreSQL is not available after multiple attempts.")


# Clean and transform weather data
def transform_weather_data(data):
    cleaned_data = []
    for entry in data:
        try:
            city = entry.get("name", "Unknown City")
            temperature = entry["main"].get("temp", 273.15)
            humidity = entry["main"].get("humidity", 50)
            weather = entry["weather"][0]["description"] if entry.get("weather") else "Unknown"
            temperature_celsius = temperature - 273.15
            feels_like_temp = temperature_celsius - (humidity / 100) * 2
            lat = entry["coord"].get("lat", 0.0)
            lon = entry["coord"].get("lon", 0.0)

            cleaned_data.append({
                "city": city,
                "temperature_celsius": round(temperature_celsius, 2),
                "humidity": humidity,
                "weather": weather,
                "feels_like_temp": round(feels_like_temp, 2),
                "latitude": lat,
                "longitude": lon
            })
        except Exception as e:
            print(f"Error cleaning weather data: {e}")
    return pd.DataFrame(cleaned_data)


# Clean and transform COVID-19 data
def transform_covid_data(data):
    cleaned_data = []
    for entry in data:
        try:
            state = entry.get("state", "Unknown State")
            positive_cases = entry.get("positive", 0)
            hospitalized = entry.get("hospitalized", 0)
            deaths = entry.get("death", 0)

            cleaned_data.append({
                "state": state,
                "positive_cases": positive_cases,
                "hospitalized": hospitalized,
                "deaths": deaths
            })
        except Exception as e:
            print(f"Error cleaning COVID data: {e}")
    return pd.DataFrame(cleaned_data)


# Clean and transform exchange rate data
def transform_exchange_rate_data(data):
    cleaned_data = []
    for base_currency, exchange_data in data.items():
        for target_currency, rate in exchange_data["rates"].items():
            cleaned_data.append({
                "base_currency": base_currency,
                "target_currency": target_currency,
                "rate": rate
            })
    return pd.DataFrame(cleaned_data)


# Clean and transform SpaceX data
def transform_spacex_data(data):
    cleaned_data = []
    for entry in data:
        try:
            mission_name = entry.get("name", "Unknown Mission")
            launch_date = entry.get("date_utc", "Unknown Date")
            rocket = entry.get("rocket", "Unknown Rocket")

            cleaned_data.append({
                "mission_name": mission_name,
                "launch_date": launch_date,
                "rocket": rocket
            })
        except Exception as e:
            print(f"Error cleaning SpaceX data: {e}")
    return pd.DataFrame(cleaned_data)


# Load data into PostgreSQL
def load_to_postgres(df, table_name):
    try:
        conn = psycopg2.connect(
            dbname="etl_database",
            user="user",
            password="password",
            host="db",
            port=5432
        )
        cursor = conn.cursor()

        # Create a table dynamically based on DataFrame columns
        column_definitions = ', '.join([f"{col} TEXT" for col in df.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});")

        for _, row in df.iterrows():
            columns = ', '.join(row.keys())
            values = ', '.join(['%s'] * len(row))
            cursor.execute(f"INSERT INTO {table_name} ({columns}) VALUES ({values})", tuple(row))

        conn.commit()
        print(f"Data successfully loaded into PostgreSQL table: {table_name}")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
    finally:
        conn.close()


# Load data into SQLite
def load_to_sqlite(df, db_path="/app/sqlite_data/etl_database.sqlite", table_name="weather_data"):
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        print(f"Data successfully loaded into SQLite table: {table_name}")
    except Exception as e:
        print(f"Error loading data to SQLite: {e}")
    finally:
        conn.close()


# Save data to multiple formats
def save_data_to_file_formats(df, base_path="/app/shared_data", table_name="data"):
    try:
        # CSV
        df.to_csv(f"{base_path}/{table_name}.csv", index=False)
        # Parquet
        df.to_parquet(f"{base_path}/{table_name}.parquet", index=False)
        # JSON
        df.to_json(f"{base_path}/{table_name}.json", orient="records")
        print(f"Data successfully saved for {table_name}")
    except Exception as e:
        print(f"Error saving data to files for {table_name}: {e}")


# Main transformation process
def transform_data():
    # Wait for the database to be ready
    wait_for_db()

    # Load raw datasets
    with open("/app/shared_data/weather_data.json", "r") as f:
        weather_data = json.load(f)
    with open("/app/shared_data/covid_data.json", "r") as f:
        covid_data = json.load(f)
    with open("/app/shared_data/exchange_rate_data.json", "r") as f:
        exchange_rate_data = json.load(f)
    with open("/app/shared_data/spacex_data.json", "r") as f:
        spacex_data = json.load(f)

    # Clean and transform datasets
    weather_df = transform_weather_data(weather_data)
    covid_df = transform_covid_data(covid_data)
    exchange_rate_df = transform_exchange_rate_data(exchange_rate_data)
    spacex_df = transform_spacex_data(spacex_data)

    # Save to multiple formats
    save_data_to_file_formats(weather_df, table_name="weather_data")
    save_data_to_file_formats(covid_df, table_name="covid_data")
    save_data_to_file_formats(exchange_rate_df, table_name="exchange_rate_data")
    save_data_to_file_formats(spacex_df, table_name="spacex_data")

    # Load into databases
    load_to_postgres(weather_df, "weather_data")
    load_to_postgres(covid_df, "covid_data")
    load_to_postgres(exchange_rate_df, "exchange_rate_data")
    load_to_postgres(spacex_df, "spacex_data")

    load_to_sqlite(weather_df, table_name="weather_data")
    load_to_sqlite(covid_df, table_name="covid_data")
    load_to_sqlite(exchange_rate_df, table_name="exchange_rate_data")
    load_to_sqlite(spacex_df, table_name="spacex_data")


if __name__ == "__main__":
    transform_data()

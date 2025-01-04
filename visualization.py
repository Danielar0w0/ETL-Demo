# Import required libraries
import sqlite3

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2

# dfbase connection parameters
DB_NAME = "etl_database"
DB_USER = "user"
DB_PASSWORD = "password"
DB_HOST = "localhost"  # If running within Docker, use "db"
DB_PORT = 5432


# Connect to PostgreSQL
def fetch_data_postgres(query):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Connect to SQLite
def fetch_data_sqlite(query, db_path="/app/shared_data/etl_database.sqlite"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# Fetch Weather Data and Plot Temperature & Humidity
def visualize_weather_data():
    query = "SELECT city, temperature_celsius, humidity, feels_like_temp FROM weather_data"
    df = fetch_data_postgres(query)

    # Convert metrics to float for plotting
    df['temperature_celsius'] = df['temperature_celsius'].astype(float)
    df['feels_like_temp'] = df['feels_like_temp'].astype(float)
    df['humidity'] = df['humidity'].astype(float)

    bar_width = 0.35
    cities = df['city']
    x = range(len(cities))

    # Bar plot for temperature and feels like temperature
    plt.bar(x, df['temperature_celsius'], width=bar_width, label='Temperature (°C)')
    plt.bar([i + bar_width for i in x], df['feels_like_temp'], width=bar_width, label='Feels Like Temp (°C)')
    plt.xlabel('City')
    plt.ylabel('Value (°C)')
    plt.title('Temperature vs Feels Like Temperature by City')
    plt.xticks([i + bar_width / 2 for i in x], cities)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Scatter plot for temperature and humidity (with annotations)
    fig, ax = plt.subplots()
    ax.scatter(df['temperature_celsius'], df['humidity'])
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Humidity (%)')
    plt.title('Temperature vs Humidity by City')
    for i, txt in enumerate(cities):
        ax.annotate(txt, (df['temperature_celsius'][i], df['humidity'][i]))
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# Fetch COVID-19 Data and Visualize Cases & Deaths
def visualize_covid_data():
    query = "SELECT state, positive_cases, deaths FROM covid_data"
    df = fetch_data_postgres(query)

    # Convert metrics to float for plotting
    df['positive_cases'] = df['positive_cases'].astype(float)
    df['deaths'] = df['deaths'].astype(float)

    bar_width = 0.35
    states = df['state']
    x = range(len(states))

    # Bar plot for positive cases and deaths
    plt.bar(x, df['positive_cases'], width=bar_width, label='Positive Cases')
    plt.bar([i + bar_width for i in x], df['deaths'], width=bar_width, label='Deaths')
    plt.xlabel('State')
    plt.ylabel('Count')
    plt.title('COVID-19 Cases and Deaths by State')
    plt.xticks([i + bar_width / 2 for i in x], states)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Pie chart for positive cases by state
    fig, ax = plt.subplots()
    ax.pie(df['positive_cases'], labels=states, autopct='%1.1f%%')
    plt.title('COVID-19 Positive Cases by State')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# Fetch Exchange Rate Data and Plot Currency Comparison
def visualize_exchange_rate_data():
    query = "SELECT base_currency, target_currency, rate FROM exchange_rate_data"
    df = fetch_data_postgres(query)

    # Convert rate to float for plotting
    df['rate'] = df['rate'].astype(float)

    for base_currency in df['base_currency'].unique():
        # Filter the dataframe by base currency
        base_df = df[df['base_currency'] == base_currency]

        # Sort the dataframe by rate
        base_df = base_df.sort_values(by='rate', ascending=True)
        x = range(len(base_df))

        # Horizontal plot for exchange rates
        plt.barh(x, base_df['rate'])
        plt.xlabel('Rate')
        plt.ylabel('Target Currency')
        plt.title(f'Exchange Rates for {base_currency}')
        plt.yticks(x, base_df['target_currency'])
        plt.tight_layout()
        plt.show()


# Fetch SpaceX Data and Plot Launch Frequency
def visualize_spacex_data():
    query = "SELECT mission_name, launch_date FROM spacex_data"
    df = fetch_data_postgres(query)

    # Convert launch_date to datetime format
    df['launch_date'] = pd.to_datetime(df['launch_date'])

    # Sorting the dataframe by launch date
    df = df.sort_values(by='launch_date')

    # Timeline plot for SpaceX missions
    plt.figure(figsize=(12, 6))
    plt.plot(df['launch_date'], range(len(df)), marker='o', linestyle='', markersize=10)

    # Adding labels to the points
    for i, row in df.iterrows():
        plt.text(row['launch_date'], i, row['mission_name'], ha='left', va='center')

    # Formatting the plot
    plt.xlabel('Launch Date')
    plt.title('Space Mission Timeline')
    plt.yticks(range(len(df)), df['mission_name'])
    plt.grid(True)
    plt.tight_layout()

    # Displaying the plot
    plt.show()

    # Bar Plot (Number of Launches per Month/Year)
    df['year'] = df['launch_date'].dt.year
    df['month'] = df['launch_date'].dt.month

    # Grouping by year and month
    monthly_launches = df.groupby(['year', 'month']).size()

    # Plotting the data
    monthly_launches.plot(kind='bar', figsize=(12, 6))
    plt.xlabel('Year, Month')
    plt.ylabel('Number of Launches')
    plt.title('SpaceX Launch Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def run_all_visualizations():
    visualize_weather_data()
    visualize_covid_data()
    visualize_exchange_rate_data()
    visualize_spacex_data()


# Run the visualizations
run_all_visualizations()

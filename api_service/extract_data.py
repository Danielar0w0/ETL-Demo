import json
import os

import requests

WEATHER_API_URL = "http://api.openweathermap.org/data/2.5/weather"
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
COVID_API_URL = "https://api.covidtracking.com/v1/states"
EXCHANGE_API_URL = "https://api.exchangerate-api.com/v4/latest"
SPACEX_API_URL = "https://api.spacexdata.com/v4/launches/upcoming"


def fetch_weather_data():
    """Fetch weather data from OpenWeather API"""

    cities = ["London", "New York", "Tokyo", "Paris", "Sydney"]

    weather_data = []
    for city in cities:
        try:
            response = requests.get(WEATHER_API_URL, params={"q": city, "appid": WEATHER_API_KEY})
            response.raise_for_status()
            data = response.json()
            weather_data.append(data)
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")

    # Save the weather data for multiple cities
    with open("/app/shared_data/weather_data.json", "w") as f:
        json.dump(weather_data, f)
    print("Weather data fetched for multiple cities.")


def fetch_covid_data():
    """Fetch COVID-19 data for multiple states"""
    states = ["ca", "ny", "tx", "fl", "wa"]
    covid_data = []

    for state in states:
        try:
            response = requests.get(f"{COVID_API_URL}/{state}/current.json")
            response.raise_for_status()
            covid_data.append(response.json())
        except Exception as e:
            print(f"Error fetching COVID-19 data for {state}: {e}")

    # Save the COVID-19 data
    with open("/app/shared_data/covid_data.json", "w") as f:
        json.dump(covid_data, f)
    print("COVID-19 data fetched for multiple states.")


def fetch_exchange_rate_data():
    """Fetch exchange rates for multiple base currencies"""
    currencies = ["USD", "EUR", "GBP"]
    exchange_data = {}

    for currency in currencies:
        try:
            response = requests.get(f"{EXCHANGE_API_URL}/{currency}")
            response.raise_for_status()
            exchange_data[currency] = response.json()
        except Exception as e:
            print(f"Error fetching exchange rate data for {currency}: {e}")

    # Save the exchange rate data
    with open("/app/shared_data/exchange_rate_data.json", "w") as f:
        json.dump(exchange_data, f)
    print("Exchange rate data fetched for multiple currencies.")


def fetch_spacex_data():
    """Fetch SpaceX upcoming and past launches"""
    try:
        response = requests.get(SPACEX_API_URL)
        response.raise_for_status()
        spacex_data = response.json()

        # Save the SpaceX data
        with open("/app/shared_data/spacex_data.json", "w") as f:
            json.dump(spacex_data, f)
        print("SpaceX launch data fetched.")
    except Exception as e:
        print(f"Error fetching SpaceX data: {e}")


def extract_data():
    """Run all extraction tasks"""
    print("Starting data extraction...")
    fetch_weather_data()
    fetch_covid_data()
    fetch_exchange_rate_data()
    fetch_spacex_data()
    print("Data extraction completed!")


if __name__ == "__main__":
    extract_data()

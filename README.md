# Docker ETL Pipeline

This project builds an ETL (Extract, Transform, Load) pipeline using Docker to extract data from multiple APIs, transform it, and load it into PostgreSQL and SQLite databases, while saving the data in CSV, JSON, and Parquet formats.

## Features
- **Data Sources:** Weather, COVID-19, Exchange Rate, SpaceX Launches
- **Transformations:** Data Cleaning, Feature Engineering
- **Loading:** PostgreSQL, SQLite, CSV, JSON, Parquet
- **Containerization:** Docker & Docker Compose

## Prerequisites
- Docker & Docker Compose
- Python 3.x

## Setup
1. Create a `.env` file in the project root (OpenWeatherMap API key required):
   ```plaintext
   WEATHER_API_KEY=your_api_key_here
   ```
2. Build and start the services:
   ```bash
   docker-compose up --build
   ```

## Visualizations
Run Python script for data exploration:
```bash
pip install -r requirements.txt
python visualizations.py
```

## Technologies Used
- **Python**
- **Docker & Docker Compose**
- **PostgreSQL & SQLite**
- **Pandas, Matplotlib**


# Docker ETL Pipeline

This project builds an ETL (Extract, Transform, Load) pipeline using Docker to extract data from multiple APIs, transform it, and load it into PostgreSQL and SQLite databases, while saving the data in CSV, JSON, and Parquet formats. Additionally, it includes a Streamlit Dashboard for data visualization.

## Features
- **Data Sources:** Weather, COVID-19, Exchange Rate, SpaceX Launches
- **Transformations:** Data Cleaning, Feature Engineering
- **Loading:** PostgreSQL, SQLite, CSV, JSON, Parquet
- **Visualization**: Streamlit Dashboard with interactive charts using Plotly
- **Containerization:** Docker & Docker Compose

## Prerequisites
- Docker & Docker Compose
- Python 3.x

## Services

- **api_service:** Extracts data from APIs
- **transform_service:** Cleans and transforms the extracted data, loading to databases
- **app_streamlit:** Streamlit dashboard for visualizing data

## Setup
1. Create a `.env` file in the project root (OpenWeatherMap API key required):
   ```plaintext
   WEATHER_API_KEY=your_api_key_here
   ```
2. Build and start the services:
   ```bash
   docker compose down -v
   docker-compose up --build
   ```

## Visualizations

Access the Streamlit Dashboard at http://localhost:8501/ for interactive data exploration.

Alternately, run Python script for data exploration:
```bash
pip install -r requirements.txt
python visualizations.py
```

## Technologies Used
- **Python**
- **Docker & Docker Compose**
- **PostgreSQL & SQLite**
- **Streamlit, Plotly, Matplotlib, Pandas**


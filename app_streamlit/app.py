import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import emoji
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(layout="centered")
st.title("Dashboard de Dados")

st.markdown("<hr>", unsafe_allow_html=True)

def load_data_from_postgres(table_name):
    try:
        conn = psycopg2.connect(
            dbname="etl_database",
            user="user",
            password="password",
            host="db",
            port=5432,
            options="-c client_encoding=UTF8"
        )
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except psycopg2.Error as e:
        st.error(f"Erro ao carregar dados de {table_name}: {e}")
        return pd.DataFrame()

def get_weather_emoji(description):
    description_map = {
        "clear sky": "☀️",
        "few clouds": "🌤️",
        "scattered clouds": "🌥️",
        "broken clouds": "☁️",
        "overcast clouds": "☁️",
        "shower rain": "🌧️",
        "rain": "🌧️",
        "thunderstorm": "⛈️",
        "snow": "❄️",
        "mist": "🌫️"
    }
    return description_map.get(description, "❓")

def get_state_name(state):
    state_map = {
        "CA": "California",
        "NY": "New York",
        "TX": "Texas",
        "FL": "Florida",
        "WA": "Washington"
    }
    return state_map.get(state, state)

data_options = {
    "weather_data": "Dados Meteorológicos",
    "covid_data": "Dados de COVID-19",
    "exchange_rate_data": "Taxas de Câmbio",
    "spacex_data": "Lançamentos da SpaceX"
}


selected_dataset = st.selectbox("Escolha o dataset:", list(data_options.keys()), format_func=lambda x: data_options[x])

df = load_data_from_postgres(selected_dataset)

st.markdown("<hr>", unsafe_allow_html=True)

if not df.empty:
    st.subheader(data_options[selected_dataset])

    if selected_dataset == "weather_data":
        cities = df["city"].unique()
        selected_cities = st.multiselect("Escolha as cidades:", cities, key="city_multiselect")

        if selected_cities:
            cols = st.columns(len(selected_cities))
            for idx, city in enumerate(selected_cities):
                city_data = df[df['city'] == city].iloc[-1] 
                weather_emoji = get_weather_emoji(city_data['weather'])
                with cols[idx]:
                    st.markdown(f"<h2 style='font-size: 20px;'>{weather_emoji} {city}</h2>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 12px;'>Temperature: {city_data['temperature_celsius']}°C</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 12px;'>Humidity: {city_data['humidity']}%</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 12px;'>Feels Like: {city_data['feels_like_temp']}°C</p>", unsafe_allow_html=True)

            filtered_df = df[df["city"].isin(selected_cities)]
            st.write(filtered_df)

            metrics = st.multiselect("Escolha as métricas que deseja ver:", ["temperature_celsius", "humidity", "feels_like_temp"], key="metrics_multiselect")

            if metrics:
                for metric in metrics:
                    filtered_df[metric] = pd.to_numeric(filtered_df[metric], errors='coerce')

                avg_df = filtered_df.groupby('city')[metrics].mean().reset_index()
                st.write(avg_df)

               
                avg_df.set_index('city', inplace=True)
                with st.container():
                    fig = px.bar(avg_df, x=avg_df.index, y=metrics, barmode='group', width=1000)
                    st.plotly_chart(fig, use_container_width=True)

    elif selected_dataset == "covid_data":
        states = df["state"].unique()
        state_names = [get_state_name(state) for state in states]
        selected_states = st.multiselect("Escolha os estados:", state_names, key="state_multiselect")

        if selected_states:

            selected_state_codes = [state for state in states if get_state_name(state) in selected_states]
            filtered_df = df[df["state"].isin(selected_state_codes)].drop_duplicates(subset="state", keep="first")
            
            numeric_columns = ["positive_cases", "hospitalized", "deaths"] 
            for column in numeric_columns:
                filtered_df[column] = pd.to_numeric(filtered_df[column], errors='coerce')

            filtered_df = filtered_df.fillna(0)

            cols = st.columns(len(selected_state_codes))
            for idx, state in enumerate(selected_state_codes):
                state_data = filtered_df[filtered_df['state'] == state].iloc[0]  
                state_name = get_state_name(state)
                with cols[idx]:
                    st.markdown(f"<h2 style='font-size: 20px;'>{state_name}</h2>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 15px;'>🦠 {state_data['positive_cases']}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 15px;'>🏥 {state_data['hospitalized']}</p>", unsafe_allow_html=True)
                    st.markdown(f"<p style='font-size: 15px;'>⚰️ {state_data['deaths']}</p>", unsafe_allow_html=True)
            
            st.write(filtered_df)

            st.markdown("<hr>", unsafe_allow_html=True)

            metrics = st.multiselect("Escolha as métricas que deseja ver:", df.columns.difference(['state']), key="covid_metrics_multiselect")

            if metrics:
                filtered_df.set_index('state', inplace=True)
                with st.container():
                    fig = px.bar(filtered_df, x=filtered_df.index, y=metrics, barmode='group', width=1000)
                    st.plotly_chart(fig, use_container_width=True)

    elif selected_dataset == "exchange_rate_data":
        base_currencies = df["base_currency"].unique()
        selected_base_currencies = st.multiselect("Escolha as moedas base:", base_currencies)
        
        if selected_base_currencies:
            filtered_df = df[df["base_currency"].isin(selected_base_currencies)].drop_duplicates()
            st.write(filtered_df)

            selected_currencies_for_chart = st.multiselect("Escolha as moedas base para o gráfico:", selected_base_currencies)
            
            if selected_currencies_for_chart:

                combined_df = pd.DataFrame()
                for currency in selected_currencies_for_chart:
                    currency_df = filtered_df[filtered_df["base_currency"] == currency]
                    combined_df = pd.concat([combined_df, currency_df])
                
                if not combined_df.empty:
                    st.markdown(f"### Conversões para {', '.join(selected_currencies_for_chart)}")
                    fig = px.bar(combined_df, x="target_currency", y="rate", color="base_currency", barmode='group', title=f"Taxas de Câmbio para {', '.join(selected_currencies_for_chart)}")
                    st.plotly_chart(fig, use_container_width=True)

    elif selected_dataset == "spacex_data":
        mission_names = df["mission_name"].unique()
        selected_missions = st.multiselect("Escolha as missões:", mission_names)
        
        if selected_missions:
            filtered_df = df[df["mission_name"].isin(selected_missions)].drop_duplicates()
            st.write(filtered_df)
            
            filtered_df['launch_date'] = pd.to_datetime(filtered_df['launch_date'])
            
            fig = px.scatter(filtered_df, x='launch_date', y='mission_name', title='Missões da SpaceX ao longo do tempo', hover_data=['mission_name'])
            fig.update_traces(marker=dict(size=10, opacity=0.8), selector=dict(mode='markers'))
            fig.update_layout(xaxis_title='Data', yaxis_title='Missão')
            
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Nenhum dado disponível para exibição.")

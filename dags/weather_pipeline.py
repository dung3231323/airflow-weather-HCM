from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
import requests
from datetime import datetime, timedelta

# Weather API setup
base_url = "http://api.weatherapi.com/v1/forecast.json"
city = "Ho Chi Minh"
days = 2  
api_key = "a0e4823fba8347678be10700240512"

params = {
    'q': city,
    'days': days,
    'key': api_key
}
default_args = {
    'owner': 'Dung',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='weather_pipeline',
    start_date=datetime(2024, 12, 5),
    schedule_interval='@daily',  
    default_args=default_args,
    catchup=False,  
)
def hcm_weather():
    @task()
    def get_weather():
        """Get weather data from WeatherAPI"""
        response = requests.get(base_url, params=params)
        print(response)
        if response.status_code == 200:
            return response.json()  
        else:
            raise Exception(f"Error fetching weather data: {response.status_code}")

    @task()
    def save_to_postgres(response):
        # Connect to PostgreSQL
        postgres_conn_id = 'weather',
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor() 

        city = response["location"]["name"],
        localtime =  response["location"]["localtime"],
        current_temp_c = response["current"]["temp_c"],
        current_wind_mph = response["current"]["wind_mph"],
        current_humidity = response["current"]["humidity"],
        current_feelslike_c = response["current"]["feelslike_c"],
        current_uv = response["current"]["uv"],
        current_chance_of_rain = response["forecast"]["forecastday"][0]["day"]["daily_chance_of_rain"]
        forecast_avgtemp =  response["forecast"]["forecastday"][1]["day"]["avgtemp_c"],
        forecast_avghumidity = response["forecast"]["forecastday"][1]["day"]["avghumidity"],
        forecast_uv = response["forecast"]["forecastday"][1]["day"]["uv"],
        forecast_chance_of_rain = response["forecast"]["forecastday"][1]["day"]["daily_chance_of_rain"]

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(100),
                local_time timestamp,
                
                current_temp_c float,
                current_wind_mph float,
                current_humidity float,
                current_feelslike_c float,
                current_uv float,

                forecast_avgtemp_c float,
                forecast_avghumidity float,
                forecast_uv float,
                forecast_change_of_rain float
            );
            INSERT INTO weather_data (
                city_name, 
                local_time, 
                current_temp_c, 
                current_wind_mph, 
                current_humidity, 
                current_feelslike_c, 
                current_uv, 
                forecast_avgtemp_c, 
                forecast_avghumidity, 
                forecast_uv, 
                forecast_change_of_rain
            )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,(city, localtime, current_temp_c, current_wind_mph, current_humidity, current_feelslike_c, current_uv, forecast_avgtemp, forecast_avghumidity, forecast_uv, forecast_chance_of_rain))
        conn.commit()
        cursor.close()

    # Task dependencies
    weather_data = get_weather()
    save_to_postgres(weather_data)
  
hcm_weather_dag = hcm_weather()
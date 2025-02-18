from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import json

def download_weather_data(city, **kwargs):
    api_key= "" #openweathermap
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

    response = requests.get(url)

    if response.status_code == 200:
        weather_data = response.json()

        city_safe = city.replace(' ', '_')
        
        kwargs['ti'].xcom_push(key=f'{city_safe}_weather_data', value = weather_data)


def parse_weather_date(city, **kwargs):

    ti = kwargs['ti']
    city_safe = city.replace(' ', '_')
    weather_data = ti.xcom_pull(task_ids=f'{city_safe}_weather', key = f'{city_safe}_weather_data')

    city_name = weather_data['name']
    summary = weather_data['weather'][0]['main']
    max_temp = weather_data['main']['temp_max']
    min_temp = weather_data['main']['temp_min']
    wind_speed = weather_data['wind']['speed']
    sunrise = weather_data['sys']['sunrise']
    sunrise_time = datetime.fromtimestamp(sunrise).strftime('%Y-%m-%d %H:%M:%S')
    sunset = weather_data['sys']['sunset']
    sunset_time = datetime.fromtimestamp(sunset).strftime('%Y-%m-%d %H:%M:%S')

    result= {
        'city' : city_name,
        'summary' : summary,
        'max_temp' : max_temp,
        'min_temp' : min_temp, 
        'wind_speed' : wind_speed,
        'sunrise_time' : sunrise_time,
        'sunset_time' : sunset_time
    }

    ti.xcom_push(key=f'{city_safe}_parsed_weather_data', value=result)
    return result


def compare_weather(**kwargs):
    ti = kwargs['ti']

    boston_data = ti.xcom_pull(task_ids = 'Boston_parse', key ='Boston_parsed_weather_data')
    charlotte_data = ti.xcom_pull(task_ids = 'Charlotte_parse', key = 'Charlotte_parsed_weather_data')
    chicago_data = ti.xcom_pull(task_ids = 'Chicago_parse', key ='Chicago_parsed_weather_data')

    coldest_city= min([boston_data, charlotte_data, chicago_data], key= lambda x: x['min_temp'])
    windest_city = max([boston_data, charlotte_data, chicago_data], key = lambda x: x['wind_speed'])

    print(f"The coldest city is {coldest_city['city']} with temperature {coldest_city['min_temp']}")
    print(f"The windest city is {windest_city['city']} with wind speed {windest_city['wind_speed']}")


default_args = {
    'owner' : 'yana',
    'start_date' : datetime(2025,2,10),
    'end_date' : datetime(2025, 4, 25)
}


with DAG(
    'Weather_dag_new',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    task1 = BashOperator(
        task_id = 'Dummy_hello',
        bash_command = 'echo "Сейчас украдем немного даты"'
    )

    task2 = PythonOperator(
        task_id = 'Boston_weather',
        python_callable = download_weather_data,
        op_args=["Boston"]
    )

    task3 = PythonOperator(
        task_id= 'Boston_parse',
        python_callable = parse_weather_date,
        op_args=["Boston"]
    )

    task4 = PythonOperator(
        task_id = 'Charlotte_weather',
        python_callable = download_weather_data,
        op_args=["Charlotte"]
    )

    task5 = PythonOperator(
        task_id= 'Charlotte_parse',
        python_callable = parse_weather_date,
        op_args=["Charlotte"]
    )

    task6 = PythonOperator(
        task_id= 'Chicago_weather',
        python_callable=download_weather_data,
        op_args=["Chicago"]

    )

    task7 = PythonOperator(
        task_id='Chicago_parse',
        python_callable=parse_weather_date,
        op_args=["Chicago"]
    )

    task8 = PythonOperator(
        task_id = 'Compare_weather',
        python_callable = compare_weather
    )

task1 >> [task2, task4, task6]

task2 >> task3
task4 >> task5
task6 >> task7

[task3, task5, task7] >> task8



  





from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pyodbc

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lush_scraper',
    default_args=default_args,
    schedule_interval='@once'
)

# Define the function to scrape the data from the website
def scrape_data():
    url = 'https://www.lushusa.com/bath/bath-bombs/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    products = soup.find_all('div', {'class': 'product-tile'})
    data = []
    for product in products:
        name = product.find('h2', {'class': 'product-name'}).text.strip()
        price = product.find('div', {'class': 'price'}).text.strip()
        data.append((name, price))
    return data

# Define the function to save the data in SQL Server
def save_data(**context):
    data = context['task_instance'].xcom_pull(task_ids='scrape_data')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-JI7AUT0;DATABASE=music;UID=zuhaib;PWD=14326786')
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO BathBombs (Name, Price) VALUES (?, ?)", data)
    conn.commit()
    cursor.close()
    conn.close()

# Define the tasks
scrape_data_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_data,
    dag=dag
)

save_data_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag,
    provide_context=True
)

# Define the task dependencies
scrape_data_task >> save_data_task

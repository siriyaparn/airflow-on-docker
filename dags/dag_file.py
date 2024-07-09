import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pymysql.cursors
import pandas as pd
import requests


class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")


# Create function
def get_data_from_db():
    # Connect to the database
    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                 port=Config.MYSQL_PORT,
                                 user=Config.MYSQL_USER,
                                 password=Config.MYSQL_PASSWORD,
                                 db=Config.MYSQL_DB,
                                 charset=Config.MYSQL_CHARSET,
                                 cursorclass=pymysql.cursors.DictCursor)


    with connection.cursor() as cursor:
        # query data from table audible_data
        sql = "SELECT * from audible_data"
        cursor.execute(sql)
        result = cursor.fetchall()
    audible_data = pd.DataFrame(result)
    audible_data.set_index("Book_ID")

    with connection.cursor() as cursor:
        # query data from table audible_transaction
        cursor.execute("SELECT * FROM audible_transaction;")
        result = cursor.fetchall()  # .fetchall() is to retrieve all data in the database
    audible_transaction = pd.DataFrame(result)

    # Join table
    transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

    # Save to csv file
    transaction.to_csv("/home/airflow/data/transaction.csv", index=False)
    

def get_data_from_api():
    url = ""
    # Make the GET request
    response = requests.get(url)
    result_conversion_rate = response.json()

    # Convert to Pandas
    conversion_rate = pd.DataFrame.from_dict(result_conversion_rate)

    # Reset index from index to data column
    conversion_rate = conversion_rate.reset_index().rename(columns={"index":"date"})

    # Convert the timestamp column to date in the transaction and conversion_rate dataframes
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # Save to csv file
    conversion_rate.to_csv("/home/airflow/data/conversion_rate_from_api.csv", index=False)


def convert_to_thb():
    transaction = pd.read_csv("/home/airflow/data/transaction.csv")
    conversion_rate = pd.read_csv("/home/airflow/data/conversion_rate_from_api.csv")

    # Copy the timestamp column and store it in a new column named date
    # This will allow the date to be converted to a format compatible for joining with the currency data.
    transaction['date'] = transaction['timestamp']

    # Convert the timestamp column to date in the transaction and conversion_rate dataframes
    transaction['date'] = pd.to_datetime(transaction['date']).dt.date
    conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

    # Join data: transaction and conversion_rate
    final_df = transaction.merge(conversion_rate, how="left", left_on="date", right_on="date")

    # Replace "$" with ""
    final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)

    # Convert the column "Price" to float
    final_df["Price"] = final_df["Price"].astype(float)

    # Calculate THBPrice
    final_df['THBPrice'] = final_df['Price'] * final_df['conversion_rate']

    # Drop columns that are not needed
    final_df = final_df.drop("date", axis=1)

    # Save to csv file
    final_df.to_csv("/home/airflow/data/result.csv", index=False)
        


# Default Args
default_args = {
    'owner': 'datath',
    'depends_on_past': False,
    'catchup': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Create DAG
dag = DAG(
    'audible_pipeline',
    default_args=default_args,
    description='Pipeline for ETL audible data',
    schedule_interval=timedelta(days=1),
)


# Tasks
t1 = PythonOperator(
    task_id='db_ingest',
    python_callable=get_data_from_db,
    dag=dag,
)

t2 = PythonOperator(
    task_id='api_call',
    python_callable=get_data_from_api,
    dag=dag,
)

t3 = PythonOperator(
    task_id='convert_currency',
    python_callable=convert_to_thb,
    dag=dag,
)


# Dependencies
[t1, t2] >> t3
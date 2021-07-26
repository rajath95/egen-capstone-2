from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import json
import pandas as pd
from google.cloud import bigquery

import logging



def extract():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('stock_storage')
    blob = bucket.blob('stock.csv')
    data = json.loads(blob.download_as_string())
    return data


def transform(**kwargs):
    task_instance = kwargs['instance']
    data = task_instance.xcom_pull(task_ids='read_file_from_bucket')
    stockInfo = data["price"]
    stockdata = data["ticker"]
    HighpriceList = []
    HighpriceStockticker= []
    LowPriceList = []
    LowpriceStockticker = []

    for k in stockInfo:
        if stockInfo[k] > 200
            Highprice.append(k)
            HighpriceStockticker.append(stockdata)
        else:
            Lowprice.append(k)
            LowpriceStockticker.append(stockdata)
    HighPriceList = {"Stock_Ticker":HighpriceStockticker, "Stock_Price": HighPriceList}
    LowPriceList = {"Stock_Ticker": LowpriceStockticker, "Stock_Price": LowPriceList}

    HPL_df = pd.DataFrame(data=HighPriceList).to_json()
    LPL_df = pd.DataFrame(data=LowPriceList).to_json()

    logging.info("Successfully trasnformed the incoming data stream")

    return HPL_df, LPL_df


def load(**kwargs):
    datasetID = "Stock_data"
    task_instance = kwargs["task_instance"]
    data = task_instance.xcom_pull(task_ids="transform_data")
    HPL_df = pd.read_json(data[0])
    LPL_df = pd.read_json(data[1])
    logging.info("Received dataframes")
    logging.info(HPL_df)
    logging.info(LPL_df)
    bqclient = bigquery.client.Client(project="elevated-summer-320003")
    dataset = bqclient.dataset(Datadump)
    dataset.location = "US"
    try:
        bqclient.create_dataset(dataset)
        bqclient.create_table("High_Price_Stock")
        bqclient.create_table("Low_Price_Stock")
    except:
        pass
    bqclient.load_table_from_dataframe(HPL_df, "elevated-summer-320003.Stock_data,High_Price_Stock")
    bqclient.load_table_from_dataframe(LPL_df, "elevated-summer-320003.Stock_data.Low_Price_Stock")








with DAG(dag_id='ETL',start_date=days_ago(1),schedule_interval=None) as dag:

    extract = PythonOperator(
        task_id='read_file',
        python_callable=extract,
        dag=dag

    )

    transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    provide_context=True,
    dag=dag
    )



    load = PythonOperator(
    task_id='transform_data',
    python_callable=load,
    provide_context=True,
    dag=dag
    )


extract >> transform >> load

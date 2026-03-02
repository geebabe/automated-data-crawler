from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from crawlers import X_crawler
from config.keywords import KEYWORDS


def run_crawler_a():
    pass # crawler_a.run(KEYWORDS)


def run_crawler_b():
    pass # crawler_b.run(KEYWORDS)


def run_crawler_c():
    pass # crawler_c.run(KEYWORDS)


with DAG(
    dag_id="daily_crawl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 23 * * *",  # 23h mỗi ngày
    catchup=False,
) as dag:

    task_a = PythonOperator(
        task_id="crawl_a",
        python_callable=run_crawler_a,
    )

    task_b = PythonOperator(
        task_id="crawl_b",
        python_callable=run_crawler_b,
    )

    task_c = PythonOperator(
        task_id="crawl_c",
        python_callable=run_crawler_c,
    )

    # chạy tuần tự (nếu cần)
    task_a >> task_b >> task_c